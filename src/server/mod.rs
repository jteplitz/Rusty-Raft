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
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::mpsc::{channel, Sender, Receiver};

use self::log::{Log, MemoryLog, Entry};
use std::time;
use std::io::Error as IoError;
use rand::distributions::{IndependentSample, Range};
use std::collections::HashMap;

// Constants
// TODO: Many of these should be overwritable by Config
const ELECTION_TIMEOUT_MIN: u64 = 150; // min election timeout wait value in m.s.
const ELECTION_TIMEOUT_MAX: u64 = 300; // min election timeout wait value in m.s.
const HEARTBEAT_INTERVAL: u64    = 75; // time between hearbeats

pub struct Config {
    // Each server has a unique 64bit integer id that and a socket address
    // These mappings MUST be identical for eaah server in the cluster
    cluster: HashMap<u64, SocketAddr>,
    leader: u64,
    me: (u64, SocketAddr),
    heartbeat_timeout: Duration,
}

impl Config {
    pub fn new (cluster: HashMap<u64, SocketAddr>, leader: u64, my_id: u64,
                my_addr: SocketAddr, heartbeat_timeout: Duration) -> Config {
        Config {
            cluster: cluster,
            leader: leader,
            me: (my_id, my_addr),
            heartbeat_timeout: heartbeat_timeout,
        }
    }

    // TODO eventually implement
    // pub fn fromFile (file: String) -> Config {
    //     
    // }
}

// States that each machine can be in!
#[derive(PartialEq, Clone)]
pub enum State {
    CANDIDATE,
    LEADER,
    FOLLOWER,
}

enum RpcType {
    APPEND_ENTRIES,
    REQUEST_VOTE,
}

//#[derive(Clone)]
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
    commit_index: u64,
    peer_addr: SocketAddr, // or some other identifying field
    success: u64,
}

#[derive(Copy, Clone)]
struct RequestVoteMessage {
    term: u64,
    candidate_id: u64,
    last_log_index: u64,
    last_log_term: u64,
}

struct RequestVoteReply {
    term: u64,
    vote_granted: u64,
}

struct ClientAppendRequest {
    entry: Entry,
}

enum PeerThreadMessage {
    AppendEntries (AppendEntriesMessage),
    RequestVote (RequestVoteMessage),
}

enum MainThreadMessage {
    AppendEntriesReply (AppendEntriesReply),
    RequestVoteReply (RequestVoteReply),
    ClientAppendRequest (ClientAppendRequest),
}

pub struct PeerHandle {
    to_peer: Sender<PeerThreadMessage>,
}

pub struct Peer {
    addr: SocketAddr,
    commit_index: u64, 
    pending_entries: Vec<Entry>,
    to_main: Sender<MainThreadMessage>,
    from_main: Receiver<PeerThreadMessage>,
}

impl Peer {
    fn start (addr: SocketAddr, to_main: Sender<MainThreadMessage>) -> PeerHandle {
        let (to_peer, from_main) = channel();
        //let (to_main, from_peer) = channel();
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
            //from_peer: from_peer,
        }
    }

    pub fn send_append_entries (&mut self, entry: AppendEntriesMessage) {
        // TODO (syd)
        // 1. Construct an empty append_entries rpc
        // 2. Copy in entries from |ro_log| if peer commit index is behind.
        unimplemented!();
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

// Store's the state that the server is currently in along with the current_term
// and current_id. This fields should all share a lock.
struct ServerState {
    // TODO: state and term must be persisted to disk
    current_state: State,
    current_term: u64,
    current_index: u64,
    last_leader_contact: Instant,
    voted_for: Option<u64>
}

// TODO: RW locks?
pub struct Server {
    state: Arc<Mutex<ServerState>>,
    log: Arc<Mutex<Log>>,
    peers: Vec<PeerHandle>,
    me: (u64, SocketAddr),
    last_heartbeat: Instant,
}

///
/// Starts up a new raft server with the given config.
/// This is mostly just a bootstrapper for now. It probably won't end up in the public API
/// As of right now this function does not return. It just runs the state machine as long as it's
/// in a live thread.
///
pub fn start_server (config: Config) -> ! {
    let (tx, rx) = channel();
    let mut server = Server::new(config, tx).unwrap();

    // NB: This thread handles all state changes EXCEPT for those that move us back into the
    // follower state from either the candidate or leader state. Those are both handled in the
    // AppendEntriesRpcHandler

    // The server starts up in a follower state. Set a timeout to become a candidate.
    loop {
        let current_state = { server.state.lock().unwrap().current_state.clone() };
        match current_state {
            State::FOLLOWER => {
                let btwn = Range::new(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX);
                let mut range = rand::thread_rng();
                // TODO(jason): Subtract time since last_leader_contact from wait time
                let timeout = Duration::from_millis(btwn.ind_sample(&mut range));
                thread::sleep(timeout);
                let state = server.state.lock().unwrap();

                let now = Instant::now();
                let last_leader_contact = state.last_leader_contact;
                if now.duration_since(last_leader_contact) >= timeout {
                    // we have not heard from the leader for one timeout duration.
                    // start an election
                    server.start_election(state);
                }
            },
            State::CANDIDATE => {
                let state = server.state.lock().unwrap();
                server.start_election(state);
                unimplemented!()
            },
            State::LEADER => {
                let heartbeat_wait = Duration::from_millis(HEARTBEAT_INTERVAL);
                let since_last_heartbeat = Instant::now()
                                               .duration_since(server.last_heartbeat);
                // TODO (sydli) : use checked_sub here (possible underflow)
                let next_heartbeat = heartbeat_wait - since_last_heartbeat;
                // Timed wait on leader message pipe.
                let message = match rx.recv_timeout(next_heartbeat) {
                    Ok(message) => message,
                    // If we timed out, just send a heartbeat.
                    Err(e) => {
                        server.send_append_entries();
                        continue;
                    },
                };
                match message {
                    MainThreadMessage::AppendEntriesReply(m) => {
                        server.aggregate_append_entries_reply(m);
                        server.update_commit_index();
                    },
                    MainThreadMessage::ClientAppendRequest(m) => {
                        server.send_append_entries();
                    },
                    _ => unimplemented!(),
                };
            },
        } // end match server.state
    } // end loop
}

impl Server {
    fn new (config: Config, tx: Sender<MainThreadMessage>) -> Result<Server, IoError> {
        let me = config.me;
        // 1. Start RPC request handlers
        let append_entries_handler: Box<RpcObject> = Box::new(
            AppendEntriesHandler { to_main: Mutex::new(tx.clone()) });
        let services = vec![
            (1, append_entries_handler),
        ];
        let mut server = RpcServer::new_with_services(services);
        try!(
            server.bind((config.me.1.ip(), config.me.1.port()))
            .and_then(|_| {
                server.repl()
            })
        );


        // TODO: Should peers be a map of id->PeerHandle. I.E. Will we ever need to send a message
        // to a specific peer?

        // 2. Start peer threads.
        let peers = config.cluster.into_iter()
            .filter(|&(id, addr)| id != me.0) // filter all computers that aren't me
            .map(|(id, addr)| Peer::start(addr, tx.clone()))
            .collect::<Vec<PeerHandle>>();

        let log = Arc::new(Mutex::new(MemoryLog::new()));

        let state = Arc::new(Mutex::new(ServerState {
            current_state: State::FOLLOWER,
            current_term: 0,
            current_index: 0,
            voted_for: None,
            last_leader_contact: Instant::now()
        }));

        // 3. Construct server state object.
        Ok(Server {
            state: state,
            log: log,
            peers: peers,
            me: me,
            last_heartbeat: Instant::now()
        })
    }

    ///
    /// Write |entry| to the log and try to replicate it across all servers.
    /// This function is non-blocking; it simply forwards AppendEntries messages
    /// to all known peers.
    ///
    fn send_append_entries(&mut self) {
        unimplemented!();
        //self.last_heartbeat = Instant::now();
    }

    ///
    /// Aggregate peer's append entries response into server state
    ///
    fn aggregate_append_entries_reply(&self, message: AppendEntriesReply) {
        unimplemented!();
    }

    ///
    /// Update commit_index count to the most recent log entry with quorum.
    ///
    fn update_commit_index(&self) {
        unimplemented!();
    }

    ///
    /// Starts a new election by requesting votes from all peers
    /// If the election is successful then the server transitions into the LEADER state
    /// Otherwise the leader stays in the candidate state.
    /// The assumption is that the caller will start another election
    ///
    fn start_election(&self, mut state: MutexGuard<ServerState>) {
        { // state lock scope
            // move the lock into this scope
            let mut locked_state = state;
            // transition to the candidate state
            let last_log_index = locked_state.current_index;
            let last_log_term = locked_state.current_term;
            locked_state.current_state = State::CANDIDATE;
            locked_state.current_term += 1;
            locked_state.current_index = 0;
            locked_state.voted_for = Some(self.me.0); // vote for ourselves
            // tell each peer to send out RequestToVote RPCs
            let request_vote_message = RequestVoteMessage {
                term: locked_state.current_term,
                candidate_id: self.me.0,
                last_log_index: last_log_index,
                last_log_term: last_log_term
            };
            let ref peers = self.peers;
            for peer in peers {
                peer.to_peer.send(PeerThreadMessage::RequestVote(request_vote_message))
                    .unwrap(); // panic if the peer thread is down
            }
        }
        unimplemented!();
    }
}

struct AppendEntriesHandler {
    to_main: Mutex<Sender<MainThreadMessage>>,
}

impl RpcObject for AppendEntriesHandler {
    fn handle_rpc (&self, params: capnp::any_pointer::Reader, result: capnp::any_pointer::Builder) 
        -> Result<(), RpcError>
    {
        // TODO: If this is a valid rpc from the leader, update last_leader_contact
        
        // TODO: If we're in the CANDIDATE state and this leader's term is >= our current term
        //
        
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
