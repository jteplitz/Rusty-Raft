extern crate capnp;

use std::sync::{atomic, Arc};
use std::net::{TcpStream};
use std::io::{Write, BufWriter, BufReader, ErrorKind};
use super::{RpcServer, RpcObject};
use super::super::{RpcError, RpcClientErrorKind};
use rpc_capnp::{rpc_request, rpc_response, math_result, math_params};
use capnp::{serialize_packed, message};
use super::super::test::{AdditionRpcHandler, start_test_rpc_server};
use std::collections::HashMap;
use std::sync::mpsc::channel;
use std::thread;

/************************/
/*   BEGIN UNIT TESTS   */
/************************/

#[test]
fn it_calls_the_registered_method() {
    let counter = Arc::new(atomic::AtomicUsize::new(0));
    let test_handler = Box::new(TestRpcHandler::new_with_counter(counter.clone()));

    let mut opcode_map = HashMap::new();
    opcode_map.insert(0i16, test_handler as Box<RpcObject>);

    // need an Arc pointer to the map for the call signature of do_rpc
    let opcode_map_ref = Arc::new(opcode_map);

    // create an addition rpc test message
    let mut message = capnp::message::Builder::new_default();
    {
        let mut rpc_request = message.init_root::<rpc_request::Builder>();
        rpc_request.set_counter(0i64);
        rpc_request.set_opcode(0i16);
        rpc_request.set_version(1i16);
        rpc_request.get_params().init_as::<math_params::Builder>();
    }

    // create a response buffer
    let mut response_msg = message::Builder::new_default();

    // easiest way to get a reader is to serialize and deserialize the request
    let mut message_buffer: Vec<u8> = Vec::new();
    serialize_packed::write_message(&mut message_buffer, &message).unwrap();

    let mut buf_reader = &mut BufReader::new(&message_buffer[..]);
    let reader = serialize_packed::read_message(buf_reader, message::ReaderOptions::new()).unwrap();

    RpcServer::do_rpc(opcode_map_ref, reader, &mut response_msg).unwrap();
    assert_eq!(counter.load(atomic::Ordering::SeqCst), 1);
}

#[test]
fn it_rejects_invalid_version_rpcs() {
    let opcode_map = HashMap::new();

    // need an Arc pointer to the map for the call signature of do_rpc
    let opcode_map_ref = Arc::new(opcode_map);

    //let server = RpcServer::new_with_services(services);

    // create an addition rpc test message
    let mut message = capnp::message::Builder::new_default();
    {
        let mut rpc_request = message.init_root::<rpc_request::Builder>();
        rpc_request.set_counter(0i64);
        rpc_request.set_opcode(0i16);
        rpc_request.set_version(0i16);
        rpc_request.get_params().init_as::<math_params::Builder>();
    }

    // create a response buffer
    let mut response_msg = message::Builder::new_default();

    // easiest way to get a reader is to serialize and deserialize the request
    let mut message_buffer: Vec<u8> = Vec::new();
    serialize_packed::write_message(&mut message_buffer, &message).unwrap();

    let mut buf_reader = &mut BufReader::new(&message_buffer[..]);
    let reader = serialize_packed::read_message(buf_reader, message::ReaderOptions::new()).unwrap();

    let err = match RpcServer::do_rpc(opcode_map_ref, reader, &mut response_msg).unwrap_err() {
        RpcError::RpcClientError(e) => e,
        _ => panic!("Incorrect error type")
    };
    assert_eq!(err.kind, RpcClientErrorKind::UnkownVersion);
}

#[test]
fn it_returns_the_response() {
    const ADDITION_OPCODE: i16  = 5i16;
    const ADDITION_PARAM_1: i32 = 14;
    const ADDITION_PARAM_2: i32 = 57;
    const ADDITION_RESULT: i32  = ADDITION_PARAM_1 + ADDITION_PARAM_2;
    const COUNTER_VAL: i64      = 231267i64;

    let addition_handler = Box::new(AdditionRpcHandler {});

    let mut opcode_map = HashMap::new();
    opcode_map.insert(ADDITION_OPCODE, addition_handler as Box<RpcObject>);

    // need an Arc pointer to the map for the call signature of do_rpc
    let opcode_map_ref = Arc::new(opcode_map);

    // create an addition rpc test message
	let message = create_test_addition_rpc(COUNTER_VAL, ADDITION_OPCODE, ADDITION_PARAM_1, ADDITION_PARAM_2);

    // create a response buffer
    let mut response_msg = message::Builder::new_default();

    // easiest way to get a reader is to serialize and deserialize the request
    let mut message_buffer: Vec<u8> = Vec::new();
    serialize_packed::write_message(&mut message_buffer, &message).unwrap();

    let mut buf_reader = &mut BufReader::new(&message_buffer[..]);
    let reader = serialize_packed::read_message(buf_reader, message::ReaderOptions::new()).unwrap();

    RpcServer::do_rpc(opcode_map_ref, reader, &mut response_msg).unwrap();

    let response_reader = response_msg.get_root_as_reader::<rpc_response::Reader>().unwrap();
    assert_eq!(response_reader.get_counter(), COUNTER_VAL);
    assert_eq!(response_reader.get_error(), false);

    let math_reader = response_reader.get_result().get_as::<math_result::Reader>().unwrap();

    assert_eq!(math_reader.get_num(), ADDITION_RESULT);
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

pub fn create_test_addition_rpc (counter: i64, opcode: i16, num1: i32, num2: i32) 
    -> message::Builder<message::HeapAllocator> 
{
    let mut message = message::Builder::new_default();
    {
        let mut rpc_request = message.init_root::<rpc_request::Builder>();
        rpc_request.set_counter(counter);
        rpc_request.set_opcode(opcode);
        rpc_request.set_version(1i16);
        let mut math_builder = rpc_request.get_params().init_as::<math_params::Builder>();
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

    let server = start_test_rpc_server(("localhost", 0));
    let port = server.get_local_addr().unwrap().port();

    // connect to the server and send the rpc
    let rpc_message = create_test_addition_rpc(COUNTER, OPCODE, NUM1, NUM2);
    let client = TcpStream::connect(("localhost", port)).unwrap();
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
    let result = response.get_result().get_as::<math_result::Reader>().unwrap();
    assert_eq!(result.get_num(), RESULT);
}

#[test]
fn it_shutsdown_when_dropped() {
    let server = start_test_rpc_server(("localhost", 0));
    let port = server.get_local_addr().unwrap().port();
    {
        // ensure the server can accept connections before shutting down
        let client = TcpStream::connect(("localhost", port)).unwrap();
    }

    drop(server);
    let client_err = TcpStream::connect(("localhost", port)).unwrap_err();
    assert_eq!(client_err.kind(), ErrorKind::ConnectionRefused);
}

#[test]
fn it_shutsdown_gracefully() {
    const COUNTER: i64 = 231267i64;
    const OPCODE: i16  = 0i16;
    const NUM1: i32    = 14;
    const NUM2: i32    = 789;
    let server = start_test_rpc_server(("localhost", 0));
    let port = server.get_local_addr().unwrap().port();

    // connect to the server 
    let client = TcpStream::connect(("localhost", port)).unwrap();
    let (tx, rx) = channel();

    // move the server into a background thread that immediatly tries to drop it
    thread::spawn(move || {
        tx.send(());
        server
    });

    // wait for background thread to spawn
    rx.recv().unwrap();

    // TODO: This test is imperfect because it's possible (although unlikely)
    // that the rpc is sent, received, and dealt with before the server shutdown starts blocking

    // send the rpc
    let rpc_message = create_test_addition_rpc(COUNTER, OPCODE, NUM1, NUM2);
    let mut writer = BufWriter::new(client.try_clone().unwrap());
    serialize_packed::write_message(&mut writer, &rpc_message).unwrap();
    writer.flush().unwrap();
}

/***************************/
/*   TEST DATA STRUCTURES  */
/***************************/
pub struct TestRpcHandler {
   counter: Arc<atomic::AtomicUsize>
}

impl TestRpcHandler {
    pub fn new_with_counter(counter: Arc<atomic::AtomicUsize>) -> TestRpcHandler {
        TestRpcHandler {counter: counter} 
    }
}

impl RpcObject for TestRpcHandler {
    fn handle_rpc (&self, _: capnp::any_pointer::Reader, _: capnp::any_pointer::Builder) 
        -> Result<(), RpcError>
    {
        self.counter.fetch_add(1, atomic::Ordering::SeqCst);
        Ok(())
    }
}

