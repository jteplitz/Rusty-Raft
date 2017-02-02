mod log;
use capnp;
use rand;
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
use std::time;
use std::io::Error as IoError;
use rand::distributions::{IndependentSample, Range};

// Constants
// TODO: Many of these should be overwritable by Config
const ELECTION_TIMEOUT_MIN: u64 = 150; // min election timeout wait value in m.s.
const ELECTION_TIMEOUT_MAX: u64 = 300; // min election timeout wait value in m.s.
const HEARBEAT_INTERVAL: u64    = 75; // time between hearbeats

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
    CANDIDATE,
    LEADER,
    FOLLOWER,
}

enum RpcType {
    APPEND_ENTRIES,
    REQUEST_VOTE,
}

struct AppendEntriesMessage {
    term: u64,
    leader_id: u64,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Entry,
    leader_commit: u64,
}

struct AppendEntriesReply {
    term: u64,
    success: u64,
}

struct RequestVoteMessage {
    term: u64,
    candidate_id: u64,
    last_log_index: u64,
    las_log_term: u64,
}

struct RequestVoteReply {
    term: u64,
    vote_granted: u64,
}

enum PeerThreadMessage {
    AppendEntries (AppendEntriesMessage),
    RequestVote (RequestVoteMessage),
}

enum PeerThreadResponse {
    AppendEntries (AppendEntriesReply),
    RequestVote (RequestVoteReply),
}

pub struct PeerHandle {
    to_peer: Sender<PeerThreadMessage>,
    from_peer: Receiver<PeerThreadMessage>,
}

pub struct Peer {
    addr: SocketAddr,
    commit_index: u64, 
    pending_entries: Vec<Entry>,
    to_main: Sender<PeerThreadMessage>,
    from_main: Receiver<PeerThreadMessage>,
}

impl Peer {
    pub fn start (addr: SocketAddr) -> PeerHandle {
        let (to_peer, from_main) = channel();
        let (to_main, from_peer) = channel();
        let commit_index = 0;
        
        thread::spawn(move || {
            let peer = Peer {
                addr: addr,
                commit_index: commit_index,
                pending_entries: vec![],
                to_main: to_main,
                from_main: from_main,
            };
            peer.main();
        });

        PeerHandle {
            to_peer: to_peer,
            from_peer: from_peer,
        }
    }

    pub fn send_append_entries (&mut self, entry: AppendEntriesMessage) {
        unimplemented!()
        // TODO (syd)
        // 1. Construct an empty append_entries rpc
        // 2. Copy in entries from |ro_log| if peer commit index is behind.
    }

    pub fn send_request_vote (&self, vote: RequestVoteMessage) {
        unimplemented!()
    }

    // Main loop for this machine to push to Peers.
    fn main (mut self) {
        loop {
            match self.from_main.recv().unwrap() { // If recv fails, we in deep shit already, so just unwrap
                PeerThreadMessage::AppendEntries(entry) => self.send_append_entries(entry),
                PeerThreadMessage::RequestVote(vote) => self.send_request_vote(vote)
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
    last_leader_contact: Instant,
    voted_for: Option<SocketAddr>
}


///
/// Starts up a new raft server with the given config.
/// This is mostly just a bootstrapper for now. It probably won't end up in the public API
/// As of right now this function does not return. It just runs the state machine as long as it's
/// in a live thread.
///
pub fn start_server(config: Config) -> ! {
    let server = Server::new(config).unwrap();

    // The server starts up in a follower state. Set a timeout to become a candidate.

    loop {
        match server.state {
            State::FOLLOWER => {
                let btwn = Range::new(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX);
                let mut range = rand::thread_rng();
                let sleep_time = btwn.ind_sample(&mut range);
                thread::sleep(time::Duration::from_millis(sleep_time));
                
                // if last_leader_contact < election timeout, start an election
            },
            State::CANDIDATE => {
                // same as above?
                unimplemented!()
            },
            State::LEADER => {
                // sleep and then send a hearbeat?
                unimplemented!()
            }
        }
    }
}

impl Server {
    pub fn new (config: Config) -> Result<Server, IoError> {
        let addr = config.addr;
        // 1. Start RPC request handlers
        let append_entries_handler: Box<RpcObject> = Box::new(AppendEntriesHandler {});
        let services = vec![
            (1, append_entries_handler),
        ];
        let mut server = RpcServer::new_with_services(services);
        try!(
            server.bind((config.addr.ip(), config.addr.port()))
            .and_then(|_| {
                server.repl()
            })
        );


        // 2. Start peer threads.
        let peers = config.cluster.into_iter()
            .filter(move |x| *x != addr) // filter all computers that aren't me
            .map(|x| Peer::start(x))
            .collect::<Vec<PeerHandle>>();

        let log = Arc::new(Mutex::new(MemoryLog::new()));

        // 3. Construct server state object.
        Ok(Server {
            state: State::FOLLOWER,
            current_term: 0,
            log: log,
            peers: peers,
            addr: addr,
            voted_for: None,
            last_leader_contact: Instant::now()
        })
    }

    fn start_election(&mut self) {
        self.state = State::CANDIDATE;

        // tell each peer to send out RequestToVote RPCs
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
