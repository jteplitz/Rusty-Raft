mod log;
use capnp;
use rand;
use raft_capnp::{append_entries, append_entries_reply,
                 request_vote, request_vote_reply};
use rpc::{RpcError};
use rpc::server::{RpcObject, RpcServer};
use std::cmp;
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
    entries: Vec<Entry>,
    leader_commit: u64,
}

struct AppendEntriesReply {
    term: u64,
    commit_index: u64,
    peer: (u64, SocketAddr),
    success: bool,
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
    id: u64,
    to_peer: Sender<PeerThreadMessage>,
    commit_index: u64,
}

pub struct Peer {
    addr: SocketAddr,
    pending_entries: Vec<Entry>,
    to_main: Sender<MainThreadMessage>,
    from_main: Receiver<PeerThreadMessage>,
}

impl Peer {
    fn start (id: (u64, SocketAddr), to_main: Sender<MainThreadMessage>) -> PeerHandle {
        let (to_peer, from_main) = channel();
        //let (to_main, from_peer) = channel();
        let commit_index = 0;
        
        thread::spawn(move || {
            let peer = Peer {
                addr: id.1,
                pending_entries: vec![],
                to_main: to_main,
                from_main: from_main,
            };
            peer.main();
        });

        PeerHandle {
            id: id.0,
            to_peer: to_peer,
            commit_index: commit_index,
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
    commit_index: u64,
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
                        if m.success {
                            server.get_peer_mut(m.peer.0).map(|peer| {
                                    if (m.commit_index > peer.commit_index) {
                                        peer.commit_index = m.commit_index;
                                    }
                            });
                            server.update_commit_index();
                        }
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
        let log = Arc::new(Mutex::new(MemoryLog::new()));
        let state = Arc::new(Mutex::new(ServerState {
            current_state: State::FOLLOWER,
            current_term: 0,
            commit_index: 0,
            voted_for: None,
            last_leader_contact: Instant::now()
        }));

        // 1. Start RPC request handlers
        let append_entries_handler: Box<RpcObject> = Box::new(
            AppendEntriesHandler {
                state: state.clone(),
                log: log.clone(),
            });
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
            .map(|(id, addr)| Peer::start((id, addr), tx.clone()))
            .collect::<Vec<PeerHandle>>();

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
        // 1. Retrieve relevant state info (locked).
        let (commit_index, current_term) = { 
            let state = self.state.lock().unwrap();
            (state.commit_index.clone(),    // = commit_index
                state.current_term.clone()) // = current_term
        };
        // 2. Retrieve relevant all relevant entries from log (locked).
        let min_peer_index = self.peers.iter().fold(commit_index,
                                 |acc, ref peer| cmp::min(acc, peer.commit_index));
        let entries = {
            let log = self.log.lock().unwrap();
            log.get_entries_from(min_peer_index).to_vec()
        };
        // 3. Construct append entries requests for all peers.
        for peer in &self.peers {
            // TODO These indices should be checked against |entries|
            let peer_index = peer.commit_index as usize;
            let peer_entries = &entries[peer_index + 1 ..];
            let last_entry = entries.get(peer_index).unwrap();
            peer.to_peer.send(PeerThreadMessage::AppendEntries(AppendEntriesMessage {
                term: current_term,
                leader_id: self.me.0,
                prev_log_index: peer.commit_index,
                prev_log_term: last_entry.term,
                entries: peer_entries.to_vec(),
                leader_commit: commit_index,
            })).unwrap(); // TODO actually do something prodcutive on error
        }
        self.last_heartbeat = Instant::now();
    }

    ///
    /// Returns a mutable reference to the peer referred to by |id|.
    /// None if no peer with |id| is found.
    ///
    fn get_peer_mut(&mut self, id: u64) -> Option<&mut PeerHandle> {
        self.peers.iter_mut().find(|peer| peer.id == id)
    }

    ///
    /// Update commit_index count to the most recent log entry with quorum.
    ///
    fn update_commit_index(&mut self) {
        // Find median of all peer commit indices.
        let mut indices: Vec<u64> = self.peers.iter().map(|ref peer| peer.commit_index.clone())
                                                     .collect();
        indices.sort();
        let new_index = *indices.get( (indices.len() - 1) / 2 ).unwrap();
        // Set new commit index if it's higher!
        let mut state = self.state.lock().unwrap();
        if state.commit_index >= new_index { return; }
        state.commit_index = new_index;
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
            let last_log_index = locked_state.commit_index;
            let last_log_term = locked_state.current_term;
            locked_state.current_state = State::CANDIDATE;
            locked_state.current_term += 1;
            locked_state.commit_index = 0;
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
    state: Arc<Mutex<ServerState>>,
    log: Arc<Mutex<Log>>,
}

impl RpcObject for AppendEntriesHandler {
    fn handle_rpc (&self, params: capnp::any_pointer::Reader, result: capnp::any_pointer::Builder) 
        -> Result<(), RpcError>
    {
        // Let's read some state first :D
        let (commit_index, term) = { 
            let state = self.state.lock().unwrap();
            (state.commit_index, state.current_term)
        };
        params.get_as::<append_entries::Reader>().map(|append_entries| {
           let mut success = false;
           // TODO: If this is a valid rpc from the leader, update last_leader_contact
           // TODO: If we're in the CANDIDATE state and this leader's term is
           //       >= our current term

           // If term doesn't match, something's wrong (incorrect leader).
           if append_entries.get_term() != term {
               return; /* TODO @Jason Wat do?? */
           }
           // Update last leader contact timestamp.
           { self.state.lock().unwrap().last_leader_contact = Instant::now() }
           // If term matches we're good to go... update state!
           if append_entries.get_prev_log_index() == commit_index {
               // Deserialize entries from RPC.
               let entries: Vec<Entry> = append_entries.get_entries().unwrap().iter()
                   .map(|entry_proto| Entry {
                       index: entry_proto.get_index(),
                       term: entry_proto.get_term(),
                       data: entry_proto.get_data().unwrap().to_vec(),
                   }).collect();
               let entries_len = entries.len() as u64;
               { // Append entries to log.
                   let mut log = self.log.lock().unwrap();
                   log.append_entries(entries);
               }
               { // March forward our commit index.
                   self.state.lock().unwrap().commit_index += entries_len;
               }
               success = true;
           }
           let mut reply = result.init_as::<append_entries_reply::Builder>();
           reply.set_success(success);
           reply.set_term(term);
       })
       .map_err(RpcError::Capnp)
    }
}
