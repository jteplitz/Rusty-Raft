extern crate rusty_raft;
extern crate rand;
extern crate capnp;
mod relay_server;
mod mock_state_machine;

use mock_state_machine::*;
use relay_server::*;
use rusty_raft::server::*;
use rusty_raft::rpc::client::*;
use rusty_raft::rpc::RpcError;
use rusty_raft::client_request;
use rusty_raft::client_request::Op;
use rusty_raft::client_request_reply;

use rand::{thread_rng, Rng};
use capnp::message::{Reader};
use capnp::serialize::OwnedSegments;

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::Duration;

// Container around a MockStateMachine Receiver and a handle
// to the raft server that is running that instance of the state machine
struct StateMachineHandle {
    rx: Receiver<Vec<u8>>,
    server_handle: ServerHandle
}

/// Starts a relay server that binds to num_servers os-assigned local ports 
fn start_relay_server (num_servers: u64) -> (RelayServer, HashMap<u64, SocketAddr>) {
    let relay_server = RelayServer::new_with_random_addresses(num_servers);
    let addrs: HashMap<u64, SocketAddr> = 
        relay_server.get_bound_addresses()
        .into_iter()
        .enumerate()
        .map(|(i, addr)| (i as u64, addr))
        .collect();

    (relay_server, addrs)
}

/// Starts up a raft server behind each port in the relay server and sets up the mapping
/// from the relay address to the raft server address.
/// Returns a StateMachineHandle that can be used to communicate with the raft server (through the
/// ServerHandle) or check on the state of the MockStateMachine
fn start_raft_servers(relay_server: &mut RelayServer, addrs: &HashMap<u64, SocketAddr>) -> Vec<StateMachineHandle> {
    const HEARTBEAT_TIMEOUT: u64 = 75;
    println!("Starting {} raft servers", addrs.len());

    (0..addrs.len() as u64)
    .map(|i| {
        // create a config object
        let config = Config::new (addrs.clone(), i, "127.0.0.1:0".to_socket_addrs().unwrap().next().unwrap(),
                Duration::from_millis(HEARTBEAT_TIMEOUT));
        let (tx, rx) = channel();
        let state_machine = Box::new(MockStateMachine::new_with_sender(tx));
        let server_handle = start_server(config, move || state_machine).unwrap();

        // map this server through the relay server
        relay_server.relay_address(*addrs.get(&i).unwrap(), server_handle.get_local_addr());
        StateMachineHandle {rx: rx, server_handle: server_handle}
    })
    .collect()
}

#[test]
/// This test is as much a sanity check on our testing code as on the raft code.
/// It just makes sure we can start up a static cluster of servers without crashing
fn it_starts_up_a_cluster() {
    const NUM_SERVERS: u64 = 3;

    let (mut relay_server, addrs) = start_relay_server(NUM_SERVERS);
    let state_machines = start_raft_servers(&mut relay_server, &addrs);

    // TODO(jason): Shut down the servers
}

/// Returns a new Rpc 
fn create_client_request(op: Op, data: &[u8]) -> Rpc {
    let mut rpc = Rpc::new(CLIENT_REQUEST_OPCODE); 
    {
        let mut param_builder = rpc.get_param_builder().init_as::<client_request::Builder>();
        param_builder.set_op(op);
        param_builder.set_data(data);
    }
    rpc
}

#[test]
/// Simple normal case test that starts up a static cluster, sends an entry,
/// and ensures that entry is recieved by all state machines
fn it_replicates_an_entry() {
    const NUM_SERVERS: u64 = 10;
    const REPLICATE_TIMEOUT: u64 = 100;
    const DATA_LENGTH: usize = 1;
    let replicate_timeout = Duration::from_millis(REPLICATE_TIMEOUT);

    let (mut relay_server, addrs) = start_relay_server(NUM_SERVERS);
    let state_machines = start_raft_servers(&mut relay_server, &addrs);

    // TODO: We need a better way of sleeping until a leader is elected
    thread::sleep_ms(300);

    // generate the client append RPC and send it to each server
    // only one should respond that they're the leader
    let data: String = thread_rng().gen_ascii_chars().take(DATA_LENGTH).collect();
    let leader_reply_iter = addrs.iter()
        .map(|(_, &to_addr)| {
            let rpc = create_client_request(Op::Write, data.as_bytes());
            rpc.send(to_addr)
        })
        .collect::<Result<Vec<Reader<OwnedSegments>>, RpcError>>()
        .unwrap()
        .into_iter()
        .filter(|msg| {
            let reply = Rpc::get_result_reader(&msg).unwrap();
            reply.get_as::<client_request_reply::Reader>().unwrap().get_success()
        });
    // only 1 server should reply as leader
    assert_eq!(leader_reply_iter.count(), 1);

    // ensure that all state machines replicated the entry 
    assert!(state_machines
                .iter()
                .map(|handle| handle.rx.recv_timeout(replicate_timeout).unwrap())
                .all(|vec| vec.iter()
                              .zip(data.clone().into_bytes())
                              .all(|(a, b)| *a == b)));
    
    // TODO(jason): Shut down the servers
}
