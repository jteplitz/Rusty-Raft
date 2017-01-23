extern crate capnp;

//use super::start_echo_server;
use std::sync::{mpsc, atomic, Arc};
use std::net::{TcpStream, Shutdown};
use std::io::{Read, Write};
use std::time::Duration;
use super::{RpcServer, RpcError, RpcClientError, RpcClientErrorKind, RpcObject};
use rpc_capnp::{rpc_request, rpc_response, math_params};
use std::vec;

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
    fn handle_rpc (&self, params: rpc_request::params::Reader, result: rpc_response::result::Builder) 
        -> Result<(), RpcError>
    {
        self.counter.fetch_add(1, atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn it_can_register_services() {
    let addition_rpc_handler: Box<RpcObject> = Box::new(AdditionRpcHandler {});
    let services = vec![
        (0i16, addition_rpc_handler) // Not sure why rust needs a hint here
    ];
    let server = RpcServer::new_with_services(services);
}

// TODO: More integration tests

/*
#[test]
fn it_echos() {
    // constants
    let port = 8080;
    let message = "Hello from\n test land!";

    // start the echo server and connect to it
    let _ = start_echo_server(port).unwrap();
    let mut client = TcpStream::connect(("localhost", port)).unwrap();

    // write to the server, send a fin, and wait for the response
    let mut result = String::new();
    let num_bytes_read = client.write_all(message.as_bytes())
    .and_then(|()| client.shutdown(Shutdown::Write))
    .and_then(|()| {
        client.read_to_string(&mut result)
    }).unwrap();

    // check that the response is correct
    assert!(num_bytes_read == message.len());
    assert!(result == message);
}*/
