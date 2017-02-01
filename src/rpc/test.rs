//! RPC Integration Tests

extern crate capnp;
use super::{RpcError};
use super::server::{RpcObject, RpcServer};
use rpc_capnp::{math_result, math_params};
use std::net::{ToSocketAddrs};
use super::client::Rpc;

#[test]
fn it_sends_rpcs() {
    const NUM1: i32    = 123;
    const NUM2: i32    = 789;
    const RESULT: i32  = NUM1 + NUM2;

    let server = start_test_rpc_server(("localhost", 0));

    let mut rpc = Rpc::new(0i16);
    {
        let mut math_builder = rpc.get_param_builder().init_as::<math_params::Builder>();
        math_builder.set_num1(NUM1);
        math_builder.set_num2(NUM2);
    }

    let response = rpc.send(("localhost", server.get_local_addr().unwrap().port())).unwrap();
    let result_reader = Rpc::get_result_reader(&response)
        .and_then(|result| {
            result.get_as::<math_result::Reader>()
            .map_err(RpcError::Capnp)
        }).unwrap();
    assert_eq!(result_reader.get_num(), RESULT);
}


/***************************/
/*   TEST DATA STRUCTURES  */
/***************************/

pub struct AdditionRpcHandler {
}

impl RpcObject for AdditionRpcHandler {
    fn handle_rpc (&self, params: capnp::any_pointer::Reader, result: capnp::any_pointer::Builder) 
        -> Result<(), RpcError>
    {
        
        params.get_as::<math_params::Reader>()
        .map(|math_params| {
            let v = math_params.get_num1() + math_params.get_num2();
            result.init_as::<math_result::Builder>().set_num(v);
        })
        .map_err(RpcError::Capnp)
    }
}

/**************************/
/*   TEST HELPER METHODS  */
/**************************/

/// Starts a local test rpc server
pub fn start_test_rpc_server<A: ToSocketAddrs> (addr: A) -> RpcServer {
    let addition_rpc_handler: Box<RpcObject> = Box::new(AdditionRpcHandler {});
    let services = vec![
        (0i16, addition_rpc_handler)
    ];
    
    let mut server = RpcServer::new_with_services(services);
    server.bind(addr).unwrap();
    
    server.repl().unwrap();
    server
}

