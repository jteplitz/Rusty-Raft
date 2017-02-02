extern crate capnp;
mod log;
use raft_capnp::{append_entries, append_entries_reply,
                 request_vote, request_vote_reply};
use rpc::{RpcError};
use rpc::server::{RpcObject, RpcServer};
use std::net::{SocketAddr};
use std::time::{Duration, Instant};
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender, Receiver};

use self::log::{Log, MemoryLog, Entry};

pub struct Config {
    cluster: Vec<SocketAddr>,
    leader: SocketAddr,
    addr: SocketAddr,
    heartbeat_timeout: Duration,
}

impl Config {
    pub fn new (cluster: Vec<SocketAddr>, leader: SocketAddr,
                addr: SocketAddr, heartbeat_timeout: Duration) -> Config {
        Config {
            cluster: cluster,
            leader: leader,
            addr: addr,
            heartbeat_timeout: heartbeat_timeout,
        }
    }

    // TODO eventually implement
    // pub fn fromFile (file: String) -> Config {
    //     
    // }
}

// States that each machine can be in!
pub enum State {
    STARTING,
    CANDIDATE,
    LEADER,
    FOLLOWER,
}

pub struct PeerThreadMessage {
    state: State,
}

pub struct PeerHandle {
    channel: Sender<PeerThreadMessage>,
    commit_index: Arc<Mutex<u64>>, // Locked access.
}

pub struct Peer {
    addr: SocketAddr,
    next_heartbeat: Instant,
    heartbeat_timeout: Duration,
    channel: Receiver<PeerThreadMessage>,
    commit_index: Arc<Mutex<u64>>, // Locked access.
    ro_log: Arc<Mutex<Log>>,
    state: State,
}

impl Peer {
    pub fn start (addr: SocketAddr, ro_log: Arc<Mutex<Log>>,
                  initial_state: State, heartbeat_timeout: Duration) -> PeerHandle {
        let (send, recv) = channel();
        let commit_index = Arc::new(Mutex::new(0));
        {
            let commit = commit_index.clone();
            thread::spawn(move || {
                let peer = Peer {
                    addr: addr,
                    next_heartbeat: Instant::now(),
                    commit_index: commit,
                    heartbeat_timeout: heartbeat_timeout,
                    channel: recv,
                    ro_log: ro_log.clone(),
                    state: initial_state,
                };
                peer.main();
            });
        }
        PeerHandle {
            channel: send.clone(),
            commit_index: commit_index.clone(),
        }
    }

    pub fn send_append_entries (&self, ref ro_log: &Arc<Mutex<Log>>) {
        // TODO (syd)
        // 1. Construct an empty append_entries rpc
        // 2. Copy in entries from |ro_log| if peer commit index is behind.
    }

    pub fn send_request_vote (&self) {
        // TODO
    }

    // Main loop for this machine to ping Peers.
    fn main (mut self) {
        loop {
            // Execute
            match self.state {
                State::CANDIDATE => self.send_request_vote(),
                State::LEADER => self.send_append_entries(&self.ro_log),
                _ => {},
            };
            // Wait on next message
            self.state = match self.state {
                // Leaders have to send heartbeats.
                State::LEADER => {
                    let received = self.channel.recv_timeout(self.heartbeat_timeout);
                    match received {
                        Ok(_) => received.unwrap().state,
                        Err(_) => self.state,
                    }
                },
                // Everyone else simply waits on the next message.
                _ => self.channel.recv().unwrap().state
            }
        }
    }
}

pub struct Server {
    state: State,
    current_term: u64,
    log: Arc<Mutex<Log>>,
    peers: Vec<PeerHandle>,
    addr: SocketAddr,
}

impl Server {
    pub fn new (config: Config) -> Server {
        let addr = config.addr;
        // 1. Start RPC request handlers
        let append_entries_handler: Box<RpcObject> = Box::new(AppendEntriesHandler {});
        let services = vec![
            (1, append_entries_handler),
        ];
        let mut server = RpcServer::new_with_services(services);
        server.bind((config.addr.ip(), config.addr.port())).unwrap();
        server.repl().unwrap();

        // 2. Start peer threads.
        let log = Arc::new(Mutex::new(MemoryLog::new()));
        let heartbeat_timeout = config.heartbeat_timeout;
        {
            let ro_log = log.clone();
            let peers = config.cluster.into_iter()
                .filter(move |x| *x != addr) // filter all computers that aren't me
                .map(move |x| Peer::start(x, ro_log.clone(),
                                          if x == addr { State::LEADER }
                                          else { State::FOLLOWER },
                                          heartbeat_timeout))
                .collect::<Vec<PeerHandle>>();

        // 3. Construct server state object.
            Server {
                state: State::STARTING,
                current_term: 0,
                log: log,
                peers: peers,
                addr: addr,
            }
        }
    }

}

struct AppendEntriesHandler {}

impl RpcObject for AppendEntriesHandler {
    fn handle_rpc (&self, params: capnp::any_pointer::Reader, result: capnp::any_pointer::Builder) 
        -> Result<(), RpcError>
    {
        // TODO (syd) implement
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
