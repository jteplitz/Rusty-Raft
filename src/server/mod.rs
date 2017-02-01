extern crate capnp;
mod log;
use self::log::{Log, MemoryLog};
use rpc::{RpcError};
use rpc::server::{RpcObject, RpcServer};
use raft_capnp::{append_entries, append_entries_reply,
                 request_vote, request_vote_reply};
use std::time::{Instant};

#[derive(PartialEq, Eq)]
pub struct SocketAddress {
    pub port: u16,
    pub hostname: String,
}

pub struct Config {
    cluster: Vec<SocketAddress>,
    leader: SocketAddress,
    addr: SocketAddress,
}

impl Config {
    pub fn new (cluster: Vec<SocketAddress>, leader: SocketAddress, addr: SocketAddress) -> Config {
        Config {
            cluster: cluster,
            leader: leader,
            addr: addr,
        }
    }

    // TODO: eventually implement
    // pub fn fromFile (file: String) -> Config {
    //     
    // }
}

// States that each machine can be in!
enum State {
    STARTING,
    CANDIDATE,
    LEADER,
    FOLLOWER,
}

pub struct Peer {
    addr: SocketAddress,
    next_heartbeat: Instant,
    commit_index: u64,
}

impl Peer {
    pub fn new (addr: SocketAddress) -> Peer {
        Peer {
            addr: addr,
            next_heartbeat: Instant::now(),
            commit_index: 0,
        }
    }

    // Main loop for this machine to ping Peers.
    pub fn main (&self) -> bool {
        // TODO Think more carefully about access patterns.
        // For instance, here we need access to this Peer, the current Log, & leader state.
        loop {
            // 1. Check machine state: am I leader?
            // 2. If so, send heartbeat if time has passed.
            // 2a.   * Piggyback any outstanding Log entries (> peer.commit_index)
            //         onto heartbeat.
        }
    }
}

pub struct Server {
    state: State,
    current_term: u64,
    log: MemoryLog,
    peers: Vec<SocketAddress>,
    addr: SocketAddress,
}

impl Server {
    pub fn new (config: Config) -> Server {
        // 1. Start RPC request handlers
        let append_entries_handler: Box<RpcObject> = Box::new(AppendEntriesHandler {});
        let services = vec![
            (1, append_entries_handler),
        ];
        let mut server = RpcServer::new_with_services(services);
        server.bind((config.addr.hostname.as_str(), config.addr.port)).unwrap();
        server.repl().unwrap();

        // 2. Start peer heartbeat threads. TODO

        // 3. Construct server state object.
        Server {
            state: State::STARTING,
            current_term: 0,
            log: MemoryLog::new(),
            peers: config.cluster,
            addr: config.addr,
        }
    }

}

struct AppendEntriesHandler {}

impl RpcObject for AppendEntriesHandler {
    fn handle_rpc (&self, params: capnp::any_pointer::Reader, result: capnp::any_pointer::Builder) 
        -> Result<(), RpcError>
    {
        // TODO: implement
        // 1. Ensure prevLogIndex matches our own.
        // 2. Append outstanding log entries.
        // 3. Construct reply.
        params.get_as::<append_entries::Reader>()
           .map(|append_entries| {
               result.init_as::<append_entries_reply::Builder>().set_success(true);
           })
           .map_err(RpcError::Capnp)
    }
}
