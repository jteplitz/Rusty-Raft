// RPC Integration tests
/*
use super::server::{RpcServer};
use super::client::{Rpc};
use super::server::test::{AdditionRpcHandler, start_test_rpc_server};
use std::net::SocketAddr;

#[test]
fn it_sends_rpcs() {
    const COUNTER: i64 = 231267i64;
    const OPCODE: i16  = 0i16;
    const NUM1: i32    = 123;
    const NUM2: i32    = 789;
    const RESULT: i32  = NUM1 + NUM2;

    let server = start_test_rpc_server(("localhost", 0));

    let rpc = Rpc::new(0i16);
    let param_builder = rpc.get_param_builder().unwrap();
    let math_builder = param_builder.init_math();
    math_builder.set_num1(NUM1);
    math_builder.set_num2(NUM2);

    let response = rpc.send(("localhost", server.get_local_addr().unwrap().port())).unwrap();
    let result_reader = Rpc::get_result_reader(&response).unwrap();
    assert_eq!(result_reader.get_num(), RESULT);
}*/
