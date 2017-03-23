extern crate rusty_raft;
extern crate rand;
extern crate capnp;
extern crate env_logger;
#[macro_use]
extern crate log;
mod relay_server;
mod mock_state_machine;

use mock_state_machine::*;
use relay_server::*;
use rusty_raft::server::{start_test_server, ServerHandle};
use rusty_raft::client::RaftConnection;
use rusty_raft::common::Config;

use rand::{thread_rng, Rng};
use rand::distributions::{IndependentSample, Range};

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::mpsc::{Receiver, channel};
use std::time::Duration;
use std::fs;

// Container around a MockStateMachine Receiver and a handle
// to the raft server that is running that instance of the state machine
struct StateMachineHandle {
    rx: Receiver<Vec<u8>>,
    server_handle: ServerHandle,
    id: u64,
    addr: SocketAddr,
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

/// Starts up a new raft server and binds it to the given address
fn start_raft_server(id: u64, addr: Option<SocketAddr>) -> StateMachineHandle 
{
    const HEARTBEAT_TIMEOUT: u64 = 75;
    const STATE_FILENAME_LEN: usize = 20;

    // create a config object
    let random_filename: String = thread_rng().gen_ascii_chars().take(STATE_FILENAME_LEN).collect();
    let state_filename = String::from("/tmp/state_") + &random_filename;
    let log_filename = String::from("/tmp/log_") + &random_filename;

    let (tx, rx) = channel();
    let state_machine = Box::new(MockStateMachine::new_with_sender(tx));
    let server_handle = {
        // TODO(jason): Only include a filename for the first server
        start_test_server(id, move || state_machine, &state_filename, &log_filename, addr).unwrap()
    };

    StateMachineHandle {rx: rx,  id: id, addr: server_handle.get_local_addr(),
        server_handle: server_handle, state_filename: state_filename, log_filename: log_filename}
}

fn start_raft_servers(num_servers: u64, bootstrap_addr: SocketAddr) -> Vec<StateMachineHandle> {
    (0..num_servers)
    .map(|i| {
        let addr;
        if i == 0 {
            addr = Some(bootstrap_addr);
        } else {
            addr = None;
        }
        start_raft_server(i, addr)
    })
    .collect()
}

fn bootstrap_raft_cluster (num_servers: u64) -> (RaftConnection, Vec<StateMachineHandle>, RelayServer) {
    let (mut relay_server, addrs) = start_relay_server(1);
    let state_machines = start_raft_servers(num_servers, addrs[&0].clone());
    //bind the first server through the relay server
    relay_server.relay_address(addrs[&0], state_machines[0].addr.clone());

    let mut raft_db = RaftConnection::new_with_session(&addrs).unwrap();

    // Issue AddServerRPCs for the other servers
    for server in state_machines.iter().skip(1) {
        raft_db.add_server(server.id, server.addr).unwrap();
    }

    (raft_db, state_machines, relay_server)
}

#[test]
/// This test is as much a sanity check on our testing code as on the raft code.
/// It just makes sure we can start up a dynamic cluster of servers without crashing
fn it_starts_up_a_cluster() {
    const NUM_SERVERS: u64 = 3;
    bootstrap_raft_cluster(NUM_SERVERS);
}

#[test]
fn it_replicates_an_entry() {
    const NUM_SERVERS: u64 = 3;
    const REPLICATE_TIMEOUT: u64 = 5000;
    const DATA_LENGTH: usize = 800;
    let replicate_timeout = Duration::from_millis(REPLICATE_TIMEOUT);

    let (mut raft_db, state_machines, _relay_server) = bootstrap_raft_cluster(NUM_SERVERS);
    assert_eq!(state_machines.len(), NUM_SERVERS as usize);

    let data: String = thread_rng().gen_ascii_chars().take(DATA_LENGTH).collect();
    assert!(raft_db.command(data.as_bytes()).is_ok());

    // ensure that all state machines replicated the entry 
    assert!(state_machines
                .iter()
                .map(|handle|handle.rx.recv_timeout(replicate_timeout).unwrap())
                .all(|vec| vec == data.clone().into_bytes()));
}

#[test]
fn it_handles_a_failure() {
    let _ = env_logger::init();
    const NUM_SERVERS: u64 = 4;
    const REPLICATE_TIMEOUT: u64 = 100;
    const DATA_LENGTH: usize = 300;
    let replicate_timeout = Duration::from_millis(REPLICATE_TIMEOUT);

    let (mut raft_db, mut state_machines, _relay_server) = bootstrap_raft_cluster(NUM_SERVERS);
    assert_eq!(state_machines.len(), NUM_SERVERS as usize);

    // bring one server offline
    let index: u64 = Range::new(0, state_machines.len()).ind_sample(&mut thread_rng()) as u64;
    println!("Killing {}", index);
    state_machines.remove(index as usize);

    // generate the client append RPC and send it 
    let data: String = thread_rng().gen_ascii_chars().take(DATA_LENGTH).collect();
    raft_db.command(data.as_bytes()).unwrap();

    // ensure that all online state machines replicated the entry 
    assert!(state_machines
                .iter()
                .filter(|handle| handle.id != index)
                .map(|handle| {
                    handle.rx.recv_timeout(replicate_timeout).unwrap()
                })
                .all(|vec| vec == data.clone().into_bytes()));
}

#[test]
// Starts a 3 server cluster and commits some entries
// then adds a new server and ensures that server receives those entries
fn it_catches_up_new_server() {
    const NUM_SERVERS: u64 = 3;
    const REPLICATE_TIMEOUT: u64 = 100;
    const DATA_LENGTH: usize = 101;
    const NUM_ENTRIES: usize = 5;
    let replicate_timeout = Duration::from_millis(REPLICATE_TIMEOUT);

    // start up a cluster
    let (mut raft_db, mut state_machines, _relay_server) = bootstrap_raft_cluster(NUM_SERVERS);
    assert_eq!(state_machines.len(), NUM_SERVERS as usize);

    // commit an entry
    let commands: Vec<Vec<u8>> = (0..NUM_ENTRIES).map(|_| {
        let mut v: Vec<u8> = vec![];
        for byte in (thread_rng().gen_ascii_chars().take(DATA_LENGTH).collect::<String>().as_bytes()).iter() {
            v.push(*byte);
        }
        v
    }).collect();

    for command in commands.clone() {
        assert!(raft_db.command(&command[..]).is_ok());
    }

    // starta new server
    state_machines.push(start_raft_server(NUM_SERVERS, None));
    // add it to the cluster
    raft_db.add_server(NUM_SERVERS, state_machines[NUM_SERVERS as usize].addr).unwrap();

    // ensure that all state machines (including the new one) replicated all entries
    assert!(state_machines
                .iter()
                .map(|handle| {
                    let mut entries = vec![];
                    for _ in 0..NUM_ENTRIES {
                        entries.push(handle.rx.recv_timeout(replicate_timeout).unwrap());
                    }
                    entries
                })
                .all(|vec| vec == commands));
}

#[test]
// Starts a 3 server cluster and commits some entries
// then adds 2 new servers and crashes 2 old servers
// ensures that the cluster can still commit entries
fn it_adds_new_server_to_cluster() {
    // TODO
}

#[test]
// Starts a 2 server cluster and commits some entries
// then adds 1 new server and crashes the old leader
// ensures that the new server still recieves entries
fn new_server_stays_in_cluster() {
    // TODO
}

fn issue_command_and_assert_ok(db: &mut RaftConnection) -> Vec<u8> {
    const DATA_LENGTH: usize = 1;
    let data: String = thread_rng().gen_ascii_chars().take(DATA_LENGTH).collect();
    assert!(db.command(data.as_bytes()).is_ok());
    data.as_bytes().to_vec()
}

fn assert_data_replicated<P>(state_machines: &Vec<StateMachineHandle>, data: &[u8],
                             predicate: P, timeout: Option<Duration>)
    where for <'r> P: FnMut(&'r &StateMachineHandle) -> bool {
    state_machines.iter()
                 .filter(predicate)
                 .map(|handle| {
                     let result = if timeout.is_some() {
                         handle.rx.recv_timeout(timeout.unwrap()).unwrap()
                     } else {
                         handle.rx.recv().unwrap()
                     };
                     result
                 })
                 .all(|vec| vec.iter()
                              .zip(data.clone())
                              .all(|(a, b)| *a == *b));
}

#[test]
fn it_catches_up_when_behind() {
    const NUM_SERVERS: u64 = 4;
    const REPLICATE_TIMEOUT: u64 = 100;

    // start up a cluster
    let (mut raft_db, mut state_machines, _relay_server) = bootstrap_raft_cluster(NUM_SERVERS);
    assert_eq!(state_machines.len(), NUM_SERVERS as usize);

    // Bring a server down.
    let index: u64 = Range::new(0, state_machines.len()).ind_sample(&mut thread_rng()) as u64;
    let address = { // Shut down server.
        let sm = state_machines.remove(index as usize);
        sm.server_handle.get_local_addr()
    };

    // Issue command.
    let data = issue_command_and_assert_ok(&mut raft_db);
    let replicate_timeout = Duration::from_millis(REPLICATE_TIMEOUT);
    // ensure that all online state machines replicated the entry 
    assert_data_replicated(&state_machines, &data, |handle| handle.id != index,
                           Some(replicate_timeout));

    trace!("[ Test ] Bringing server back online.");
    // Bring server back up.
    state_machines.insert(index as usize, start_raft_server(index, None));
    // Make sure the zombie server got the message.
    assert_data_replicated(&state_machines, &data, |handle| handle.id == index, None);
    { // Issue command again.
        let data = issue_command_and_assert_ok(&mut raft_db);
        // ensure that ALL state machines replicated the entry 
        assert_data_replicated(&state_machines, &data, |_| true, None);
    }
}
