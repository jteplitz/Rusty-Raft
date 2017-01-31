extern crate capnp;

use std::sync::{atomic, Arc};
use std::net::{TcpStream, ToSocketAddrs};
use std::io::{Write, BufWriter, BufReader};
use super::{RpcServer, RpcObject};
use super::super::{RpcError, RpcClientError, RpcClientErrorKind};
use rpc_capnp::{rpc_request, rpc_response};
use capnp::{serialize_packed, message};

// handy Rpc structs for simple tests
pub struct AdditionRpcHandler {
}

impl RpcObject for AdditionRpcHandler {
    fn handle_rpc (&self, params: rpc_request::params::Reader, result: rpc_response::result::Builder) 
        -> Result<(), RpcError>
    {
        let val = try!(match params.which() {
            Ok(rpc_request::params::Which::Math(params)) => params.map_err(RpcError::Capnp),
            //Err(capnp::NotInSchema(e)) => RpcError::Capnp(capnp::NotInSchema(e)),
            _ => Err(RpcError::RpcClientError(RpcClientError::new(RpcClientErrorKind::InvalidParameters)))
        }
        .map(|math_params| {
            math_params.get_num1() + math_params.get_num2()
        }));

        result.init_math().set_num(val);
        Ok(())
    }
}

pub struct TestRpcHandler {
   counter: Arc<atomic::AtomicUsize>
}

impl TestRpcHandler {
    pub fn new_with_counter(counter: Arc<atomic::AtomicUsize>) -> TestRpcHandler {
        TestRpcHandler {counter: counter} 
    }
}

impl RpcObject for TestRpcHandler {
    fn handle_rpc (&self, _: rpc_request::params::Reader, _: rpc_response::result::Builder) 
        -> Result<(), RpcError>
    {
        self.counter.fetch_add(1, atomic::Ordering::SeqCst);
        Ok(())
    }
}

/******************************/
/*  BEGIN INTEGRATION TESTS  */
/******************************/

#[test]
fn it_can_register_services() {
    let addition_rpc_handler: Box<RpcObject> = Box::new(AdditionRpcHandler {});
    let services = vec![
        (0i16, addition_rpc_handler) 
    ];
    RpcServer::new_with_services(services);
}

/// Starts a local test rpc server on port 8080
fn start_test_rpc_server<A: ToSocketAddrs> (addr: A) -> RpcServer {
    let addition_rpc_handler: Box<RpcObject> = Box::new(AdditionRpcHandler {});
    let services = vec![
        (0i16, addition_rpc_handler)
    ];
    
    let mut server = RpcServer::new_with_services(services);
    server.bind(addr).unwrap();
    
    server.repl().unwrap();
    server
}

pub fn create_test_addition_rpc (counter: i64, opcode: i16, num1: i32, num2: i32) 
    -> message::Builder<message::HeapAllocator> 
{
    let mut message = message::Builder::new_default();
    {
        let mut rpc_request = message.init_root::<rpc_request::Builder>();
        rpc_request.set_counter(counter);
        rpc_request.set_opcode(opcode);
        rpc_request.set_version(1i16);
        let mut math_builder = rpc_request.init_params().init_math();
        math_builder.set_num1(num1);
        math_builder.set_num2(num2);
    }
    message
}

#[test]
fn it_sends_back_the_result() {
    const COUNTER: i64 = 231267i64;
    const OPCODE: i16  = 0i16;
    const NUM1: i32    = 14;
    const NUM2: i32    = 789;
    const RESULT: i32  = NUM1 + NUM2;

    start_test_rpc_server(("localhost", 8080));

    // connect to the server and send the rpc
    let rpc_message = create_test_addition_rpc(COUNTER, OPCODE, NUM1, NUM2);
    let client = TcpStream::connect(("localhost", 8080)).unwrap();
    let mut writer = BufWriter::new(client.try_clone().unwrap());
    serialize_packed::write_message(&mut writer, &rpc_message).unwrap();
    writer.flush().unwrap();

    // create a response message to store the response value and metadata
    let mut reader = BufReader::new(client);
    // create a message reader from the stream
    let response_msg = serialize_packed::read_message(&mut reader, capnp::message::ReaderOptions::new())
                           .unwrap();
    let response = response_msg.get_root::<rpc_response::Reader>().unwrap();
    assert_eq!(response.get_counter(), COUNTER);
    assert_eq!(response.get_error(), false);
    let result_reader = response.get_result();
    assert_eq!(result_reader.has_math(), true);
    let math_result = match result_reader.which() {
        Ok(rpc_response::result::Which::Math(v)) => v.unwrap(),
        _ => panic!("Invalid RPC response")
    };
    assert_eq!(math_result.get_num(), RESULT);
}
