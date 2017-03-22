extern crate rusty_raft;
extern crate rand;
extern crate capnp;
mod relay_server;
mod mock_state_machine;

use mock_state_machine::*;
use relay_server::*;
use rusty_raft::server::*;
use rusty_raft::common::*;
use rusty_raft::common::constants::*;
use rusty_raft::rpc::client::*;
use rusty_raft::rpc::RpcError;
use rusty_raft::client_request;

use rand::{thread_rng, Rng};
use rand::distributions::{IndependentSample, Range};
use capnp::message::{Reader};
use capnp::serialize::OwnedSegments;

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::Duration;
use std::fs;

// TODO: Ensure directory exists?
const TEST_STATE_DIR: &'static str = "build/state";

// Container around a MockStateMachine Receiver and a handle
// to the raft server that is running that instance of the state machine
struct StateMachineHandle {
    rx: Receiver<Vec<u8>>,
    server_handle: ServerHandle,
    id: u64,
    state_filename: String,
    log_filename: String
}

impl Drop for StateMachineHandle {
    /// Deletes the state and log files
    fn drop (&mut self) {
        fs::remove_file(&self.state_filename).unwrap();
        fs::remove_file(&self.log_filename).unwrap();
    }
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
    const STATE_FILENAME_LEN: usize = 20;
    println!("Starting {} raft servers", addrs.len());

    (0..addrs.len() as u64)
    .map(|i| {
        // create a config object
        let mut random_filename: String = thread_rng().gen_ascii_chars().take(STATE_FILENAME_LEN).collect();
        let state_filename = String::from("/tmp/state_") + &random_filename;
        let log_filename = String::from("/tmp/log_") + &random_filename;

        let (tx, rx) = channel();
        let state_machine = Box::new(MockStateMachine::new_with_sender(tx));
        let server_handle = {
            let config = Config::new (addrs.clone(), i, "127.0.0.1:0".to_socket_addrs().unwrap().next().unwrap(),
                    Duration::from_millis(HEARTBEAT_TIMEOUT), state_filename.clone(), &log_filename);
            start_server(config, move || state_machine).unwrap()
        };

        // map this server through the relay server
        relay_server.relay_address(*addrs.get(&i).unwrap(), server_handle.get_local_addr());
        StateMachineHandle {rx: rx, server_handle: server_handle, id: i,
                            state_filename: state_filename, log_filename: log_filename}
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
}

/// Returns a new Rpc 
fn create_client_request(request: client_command::Request) -> Rpc {
    let mut rpc = Rpc::new(CLIENT_REQUEST_OPCODE); 
    {
        let mut param_builder = rpc.get_param_builder().init_as::<client_request::Builder>();
        client_command::request_to_proto(request, &mut param_builder);
    }
    rpc
}

/// Sends the given data as a write request to all
/// nodes and returns a vector over the succesfull responses.
/// You should only have one response from the leader
/// Optionally skips sending to the skip id
#[cfg(test)]
fn send_client_request(addrs: &HashMap<u64, SocketAddr>, data: &[u8], skip_id: Option<u64>) 
    -> Vec<Reader<OwnedSegments>> 
{
    addrs.iter()
    .filter(|&(i, _)| {
        skip_id.is_none() || *i != skip_id.unwrap()
    })
    .map(|(_, &to_addr)| {
        let rpc = create_client_request(
            client_command::Request::Command(
                raft_command::Request::StateMachineCommand{
                    data: data.to_vec(),
                    // TODO (sydli): Actually ask for a real session here
                    session: mock_session()}));
        rpc.send(to_addr)
    })
    .collect::<Result<Vec<Reader<OwnedSegments>>, RpcError>>()
    .unwrap()
    .into_iter()
    .filter(|msg| {
        let reply = Rpc::get_result_reader(&msg).unwrap();
        client_command::reply_from_proto(
            &mut reply.get_as::<client_request::reply::Reader>().unwrap()).is_ok()
    })
    .collect::<Vec<Reader<OwnedSegments>>>()
}

#[test]
/// Simple normal case test that starts up a static cluster, sends an entry,
/// and ensures that entry is recieved by all state machines
fn it_replicates_an_entry() {
    const NUM_SERVERS: u64 = 8;
    const REPLICATE_TIMEOUT: u64 = 100;
    const DATA_LENGTH: usize = 1;
    let replicate_timeout = Duration::from_millis(REPLICATE_TIMEOUT);

    let (mut relay_server, addrs) = start_relay_server(NUM_SERVERS);
    let state_machines = start_raft_servers(&mut relay_server, &addrs);

    // TODO: We need a better way of sleeping until a leader is elected
    // Currently we just allow for 3 rounds of split votes...
    thread::sleep_ms(1000);

    // generate the client append RPC and send it to each server
    // only one should respond that they're the leader
    let data: String = thread_rng().gen_ascii_chars().take(DATA_LENGTH).collect();
    let leader_replies = send_client_request(&addrs, data.as_bytes(), None);
    // only 1 server should reply as leader
    assert_eq!(leader_replies.len(), 1);

    // ensure that all state machines replicated the entry 
    assert!(state_machines
                .iter()
                .map(|handle|handle.rx.recv_timeout(replicate_timeout).unwrap())
                .all(|vec| vec.iter()
                              .zip(data.clone().into_bytes())
                              .all(|(a, b)| *a == b)));
}

#[test]
fn it_handles_a_failure() {
    const NUM_SERVERS: u64 = 4;
    const REPLICATE_TIMEOUT: u64 = 100;
    const DATA_LENGTH: usize = 1;
    let replicate_timeout = Duration::from_millis(REPLICATE_TIMEOUT);

    let (mut relay_server, mut addrs) = start_relay_server(NUM_SERVERS);
    let mut state_machines = start_raft_servers(&mut relay_server, &addrs);

    // bring one server offline
    let index: u64 = Range::new(0, state_machines.len()).ind_sample(&mut thread_rng()) as u64;
    addrs.remove(&index);
    state_machines.remove(index as usize);
    //relay_server.set_address_active(addrs[offline_id], false);
    println!("Brought {} offline", index);

    // TODO: We need a better way of sleeping until a leader is elected
    // Currently we just allow for 3 rounds of split votes...
    thread::sleep_ms(1000);

    // generate the client append RPC and send it to each server
    // only one should respond that they're the leader
    let data: String = thread_rng().gen_ascii_chars().take(DATA_LENGTH).collect();
    // TODO: Skip offline server
    let leader_replies = send_client_request(&addrs, data.as_bytes(), Some(index));
    // only 1 server should reply as leader
    assert_eq!(leader_replies.len(), 1);

    // ensure that all online state machines replicated the entry 
    assert!(state_machines
                .iter()
                .filter(|handle| handle.id != index)
                .map(|handle|handle.rx.recv_timeout(replicate_timeout).unwrap())
                .all(|vec| vec.iter()
                              .zip(data.clone().into_bytes())
                              .all(|(a, b)| *a == b)));
}
