mod log;
use capnp;
use rand;
use raft_capnp::{append_entries, append_entries_reply,
                 request_vote, request_vote_reply};
use rpc::{RpcError};
use rpc::client::Rpc;
use rpc::server::{RpcObject, RpcServer};
use std::cmp;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender, Receiver};

use self::log::{Log, MemoryLog, Entry};
use std::io::Error as IoError;
use rand::distributions::{IndependentSample, Range};
use std::collections::HashMap;

// Constants
// TODO: Many of these should be overwrittable by Config
const ELECTION_TIMEOUT_MIN: u64 = 150; // min election timeout wait value in m.s.
const ELECTION_TIMEOUT_MAX: u64 = 300; // min election timeout wait value in m.s.
const APPEND_ENTRIES_OPCODE: i16 = 0;
const REQUEST_VOTE_OPCODE: i16 = 1;

pub struct Config {
    // Each server has a unique 64bit integer id that and a socket address
    // These mappings MUST be identical for eaah server in the cluster
    cluster: HashMap<u64, SocketAddr>,
    me: (u64, SocketAddr),
    heartbeat_timeout: Duration,
}

impl Config {
    pub fn new (cluster: HashMap<u64, SocketAddr>, my_id: u64,
                my_addr: SocketAddr, heartbeat_timeout: Duration) -> Config {
        Config {
            cluster: cluster,
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
#[derive(PartialEq, Clone, Copy)]
enum State {
    Candidate { num_votes: usize, start_time: Instant },
    Leader { last_heartbeat: Instant },
    Follower,
}

///
/// Messages for peer background threads to push to associated machines.
enum PeerThreadMessage {
    AppendEntries (AppendEntriesMessage),
    RequestVote (RequestVoteMessage),
}

//#[derive(Clone)
#[allow(dead_code)] // TODO(sydli): Remove once we're using the message
struct AppendEntriesMessage {
    term: u64,
    leader_id: u64,
    prev_log_index: usize,
    prev_log_term: u64,
    entries: Vec<Entry>,
    leader_commit: usize,
}

#[derive(Copy, Clone)]
struct RequestVoteMessage {
    term: u64,
    candidate_id: u64,
    last_log_index: usize,
    last_log_term: u64,
}

///
/// Messages that can be sent to the main thread.
/// *Reply messages encapsulate replies from peer machines, and
/// ClientAppendRequest is a message sent by the client thread.
struct AppendEntriesReply {
    term: u64,
    commit_index: usize,
    peer: (u64, SocketAddr),
    success: bool,
}

struct RequestVoteReply {
    term: u64,
    vote_granted: bool,
}

enum MainThreadMessage {
    AppendEntriesReply (AppendEntriesReply),
    RequestVoteReply (RequestVoteReply),
    ClientAppendRequest,
}

/// Handle for main thread to communicate with Peer.
struct PeerHandle {
    id: u64,
    to_peer: Sender<PeerThreadMessage>,
    commit_index: usize,
}

/// A background thread whose job is to communicate and relay messages between
/// the main thread and the peer at |addr|.
struct Peer {
    id: u64,
    addr: SocketAddr,
    to_main: Sender<MainThreadMessage>,
    from_main: Receiver<PeerThreadMessage>
}

impl Peer {
    ///
    /// Spawns a new Peer in a background thread to communicate with the server at id.
    ///
    /// # Panics
    /// Panics if the OS fails to create a new background thread.
    ///
    fn start (id: (u64, SocketAddr), to_main: Sender<MainThreadMessage>) -> PeerHandle {
        let (to_peer, from_main) = channel();
        let commit_index = 0;
        
        thread::spawn(move || {
            let peer = Peer {
                id: id.0,
                addr: id.1,
                to_main: to_main,
                from_main: from_main
            };
            peer.main();
        });

        PeerHandle {
            id: id.0,
            to_peer: to_peer,
            commit_index: commit_index,
        }
    }

    ///
    /// Sends the appropriate append entries RPC to this peer.
    ///
    /// # Panics
    /// Panics if proto fails to initialize.
    ///
    fn send_append_entries (&mut self, entry: AppendEntriesMessage) {
        let mut rpc = Rpc::new(APPEND_ENTRIES_OPCODE);
        {
            let mut params = rpc.get_param_builder().init_as::<append_entries::Builder>();
            params.set_term(entry.term);
            params.set_leader_id(entry.leader_id);
            params.set_prev_log_index(entry.prev_log_index as u64);
            params.set_prev_log_term(entry.prev_log_term);
            params.set_leader_commit(entry.leader_commit as u64);
            params.borrow().init_entries(entry.entries.len() as u32);
            for i in 0..entry.entries.len() {
                let mut entry_builder = params.borrow().get_entries().unwrap().get(i as u32);
                entry.entries[i].into_proto(&mut entry_builder);
            }
        }
        let (term, success) = rpc.send(self.addr).and_then(|msg| {
            Rpc::get_result_reader(&msg).and_then(|result| {
                result.get_as::<append_entries_reply::Reader>()
                      .map_err(RpcError::Capnp)
                })
                .map(|reply_reader| {
                    let term = reply_reader.get_term();
                    let success = reply_reader.get_success();
                    (term, term == entry.term && success)
                })
        }).unwrap_or((entry.term, false));
        let new_commit_index = entry.prev_log_index + entry.entries.len();
        let reply = AppendEntriesReply {
            term: term,
            commit_index: if success { new_commit_index } else { entry.prev_log_index },
            peer: (self.id, self.addr),
            success: success,
        };
        self.to_main.send(MainThreadMessage::AppendEntriesReply(reply)).unwrap();
}

    ///
    /// Requests a vote in the new term from this peer.
    ///
    /// # Panics
    /// Panics if the main thread has panicked or been deallocated
    ///
    fn send_request_vote (&self, vote: RequestVoteMessage) {
        let mut rpc = Rpc::new(REQUEST_VOTE_OPCODE);
        {
            let mut params = rpc.get_param_builder().init_as::<request_vote::Builder>();
            params.set_term(vote.term);
            params.set_candidate_id(vote.candidate_id);
            params.set_last_log_index(vote.last_log_index as u64);
            params.set_last_log_term(vote.last_log_term);
        }
        let vote_granted = rpc.send(self.addr)
            .and_then(|msg| {
                Rpc::get_result_reader(&msg)
                    .and_then(|result| {
                        result.get_as::<request_vote_reply::Reader>()
                              .map_err(RpcError::Capnp)
                    })
                    .map(|reply_reader| {
                        let term = reply_reader.get_term();
                        let vote_granted = reply_reader.get_vote_granted();
                        term == vote.term && vote_granted
                    })
            })
            .unwrap_or(false);
        let reply = RequestVoteReply {
            term: vote.term,
            vote_granted: vote_granted
        };
        // Panics if the main thread has panicked or been deallocated
        self.to_main.send(MainThreadMessage::RequestVoteReply(reply)).unwrap();
    }

    ///
    /// Main loop for background thread
    /// Waits on messages from its pipe and acts on them
    ///
    /// # Panics
    /// Panics if the main thread has panicked or been deallocated
    ///
    fn main (mut self) {
        loop {
            match self.from_main.recv().unwrap() {
                PeerThreadMessage::AppendEntries(entry) => self.send_append_entries(entry),
                PeerThreadMessage::RequestVote(vote) => self.send_request_vote(vote)
            }
        }
    }
}

/// Store's the state that the server is currently in along with the current_term
/// and current_id. These fields should all share a lock.
struct ServerState {
    // TODO: state and term must be persisted to disk
    current_state: State,
    current_term: u64,
    commit_index: usize,
    last_leader_contact: Instant,
    voted_for: Option<u64>,
    election_timeout: Duration
}

/// 
/// Implements state transitions
///
impl ServerState {
    // TODO: We should read through these state transitions carefully together @sydli
    ///
    /// Transitions into the candidate state by incrementing the current_term and resetting the
    /// current_index.
    /// This should only be called if we're in the follower state or already in the candidate state
    ///
    fn transition_to_candidate(&mut self, my_id: u64) {
        debug_assert!(self.current_state == State::Follower ||
                      matches!(self.current_state, State::Candidate { .. }));
        self.current_state = State::Candidate { start_time: Instant::now(), num_votes: 1 }; // we always start with 1 vote from ourselves
        self.current_term += 1;
        self.voted_for = Some(my_id); // vote for ourselves
        self.election_timeout = generate_election_timeout();
    }

    ///
    /// Transitions into the leader state from the candidate state.
    ///
    /// TODO: Persist term information to disk?
    fn transition_to_leader(&mut self, info: &mut ServerInfo, log: Arc<Mutex<Log>>) {
        debug_assert!(matches!(self.current_state, State::Candidate{ .. }));
        self.current_state = State::Leader { last_heartbeat: Instant::now() };
        { // Scope the log lock so we drop it immediately afterwards.
            log.lock().unwrap().append_entry(Entry::noop(self.current_term));
        }
        send_append_entries(info, self, log.clone());
    }

    fn transition_to_follower(&mut self, new_term: u64) {
        debug_assert!(matches!(self.current_state, State::Candidate{ .. }) ||
                      matches!(self.current_state, State::Leader{ .. }));
        self.current_term = new_term;
        self.current_state = State::Follower;
        self.voted_for = None;
        self.election_timeout = generate_election_timeout();
        // TODO: We need to stop the peers from continuing to send AppendEntries here.
    }
}

// TODO: RW locks?
// NB: State lock should never be acquired while holding the log lock
pub struct Server {
    // TODO: Rename properties?
    state: Arc<Mutex<ServerState>>,
    log: Arc<Mutex<Log>>,
    info: ServerInfo
}

struct ServerInfo {
    peers: Vec<PeerHandle>,
    me: (u64, SocketAddr),
    heartbeat_timeout: Duration,
}

impl ServerInfo {
    ///
    /// Returns a mutable reference to the peer referred to by |id|.
    /// None if no peer with |id| is found.
    ///
    fn get_peer_mut(&mut self, id: u64) -> Option<&mut PeerHandle> {
        self.peers.iter_mut().find(|peer| peer.id == id)
    }
}

///
/// Updates the server's commit index to the median of our peers' indices.
///
fn update_commit_index(server_info: &ServerInfo, state: &mut ServerState) {
    // Find median of all peer commit indices.
    let mut indices: Vec<usize> = server_info.peers.iter().map(|ref peer| peer.commit_index.clone()).collect();
    indices.sort();
    let new_index = *indices.get( (indices.len() - 1) / 2 ).unwrap();
    // Set new commit index if it's higher!
    if state.commit_index >= new_index { return; }
    state.commit_index = new_index;
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

    loop {
        let current_timeout;
        { // state lock scope
            let mut state = server.state.lock().unwrap();

            match state.current_state {
                State::Follower => {
                    let now = Instant::now();
                    current_timeout = state.election_timeout - now.duration_since(state.last_leader_contact)
                }
                State::Candidate{start_time, ..} => {
                    let time_since_election = Instant::now() - start_time;

                    // TODO: Replace explicit underflow checks with checked_sub once rust 1.16
                    // is out. Scheduled for March 16th 2017
                    if time_since_election > state.election_timeout {
                        // We timed out in between recieving a message and blocking on the message
                        // pipe. Start a new election
                        current_timeout = server.start_election(&mut state);
                    } else {
                        current_timeout = state.election_timeout - time_since_election;
                    }
                },
                State::Leader{last_heartbeat} => {
                    let heartbeat_timeout = server.info.heartbeat_timeout;
                    // TODO: Should last_heartbeat be in the leader state?
                    let since_last_heartbeat = Instant::now()
                                                   .duration_since(last_heartbeat);
                    // TODO: Replace explicit underflow checks with checked_sub once rust 1.16
                    // is out. Scheduled for March 16th 2017
                    if since_last_heartbeat > heartbeat_timeout {
                        // We timed out in between recieving a message and blocking on the message
                        // pipe. Send a heartbeat,
                        send_append_entries(&mut server.info, &mut state, server.log.clone());
                        current_timeout = heartbeat_timeout;
                    } else {
                        current_timeout = heartbeat_timeout - since_last_heartbeat;
                    }
                },
            } // end match server.state
        } // release state lock

        let message = match rx.recv_timeout(current_timeout) {
            Ok(message) => message,
            Err(_) => {
                handle_timeout(&mut server);
                continue;
            },
        };

        // Acquire state lock
        let ref mut state = * server.state.lock().unwrap();
        match message {
            MainThreadMessage::AppendEntriesReply(m) => 
                handle_append_entries_reply(m, &mut server.info, state),
            MainThreadMessage::ClientAppendRequest => 
                // TODO: Rpc handler needs to already append message to log
                send_append_entries(&mut server.info, state, server.log.clone()),
            MainThreadMessage::RequestVoteReply(m) => 
                handle_request_vote_reply(m, &mut server.info, state, server.log.clone())
        };
    } // end loop
}

///
/// Handles timeouts on the main thread.
/// Timeouts are handled differently based on our current state.
/// If we're a follower or candidate we start a new election
/// If we're the leader we send a hearbeat
///
/// # Panics
/// Panics if any of the peer threads have panicked.
///
fn handle_timeout(server: &mut Server) {
    let ref mut state = * server.state.lock().unwrap();
    match state.current_state {
        State::Follower | State::Candidate{ .. } => {
            server.start_election(state);
        },
        State::Leader{ .. } => send_append_entries(&mut server.info, state, server.log.clone())
    }
}

///
/// If we're leader, handle replies from peers who respond to AppendEntries.
/// If we hear about a term greater than ours, step down.
/// If we're Candidate or Follower, we drop the message.
/// 
fn handle_append_entries_reply(m: AppendEntriesReply, server_info: &mut ServerInfo, state: &mut ServerState) {
    match state.current_state {
        State::Leader{ .. } => {
            if m.term > state.current_term {
                state.transition_to_follower(m.term);
            } else if m.success {
                server_info.get_peer_mut(m.peer.0).map(|peer| {
                        if m.commit_index > peer.commit_index {
                            peer.commit_index = m.commit_index;
                        }
                });
                update_commit_index(server_info, state);
            }
        },
        State::Candidate{ .. } | State::Follower => ()
    }
}

///
/// Write |entry| to the log and try to replicate it across all servers.
/// This function is non-blocking; it simply forwards AppendEntries messages
/// to all known peers.
///
fn send_append_entries(info: &mut ServerInfo, state: &mut ServerState, log: Arc<Mutex<Log>>) {
    // TODO This needs to return an Error or something if we're not the leader
    
    // 1. Retrieve relevant state info (locked).
    let (commit_index, current_term) = { 
        // let state = state; // move state into scope, so it gets dropped after we get the info
        (state.commit_index,    // = commit_index
            state.current_term) // = current_term
    };
    // 2. Retrieve all relevant entries from log (locked).
    let min_peer_index = info.peers.iter().fold(commit_index,
                             |acc, ref peer| cmp::min(acc, peer.commit_index));
    let entries = {
        let log = log.lock().unwrap();
        log.get_entries_from(min_peer_index).to_vec()
    };
    // 3. Construct append entries requests for all peers.
    for peer in &info.peers {
        // TODO These indices should be checked against |entries|
        let peer_index = peer.commit_index as usize;
        let peer_entries = &entries[peer_index ..];
        let last_entry = entries.get(peer_index).unwrap();
        peer.to_peer.send(PeerThreadMessage::AppendEntries(AppendEntriesMessage {
            term: current_term,
            leader_id: info.me.0,
            prev_log_index: peer.commit_index,
            prev_log_term: last_entry.term,
            entries: peer_entries.to_vec(),
            leader_commit: commit_index,
        })).unwrap(); // TODO actually do something prodcutive on error
    }
    state.current_state = State::Leader { last_heartbeat: Instant::now() };
}

///
/// Handles replies to RequestVote Rpc
///
fn handle_request_vote_reply(reply: RequestVoteReply, info: &mut ServerInfo,
                             state: &mut ServerState, log: Arc<Mutex<Log>>) {
    // Since we can't have two mutable borrows on |state| at once we gotta
    // update votes & do the transition check in separate scopes.
    if reply.term == state.current_term && reply.vote_granted {
        if let State::Candidate{ref mut num_votes, ..} = state.current_state {
            *num_votes += 1;
        }
    }
    if let State::Candidate{num_votes, ..} = state.current_state {
        if num_votes > info.peers.len() / 2 {
            // Woo! We won the election
            state.transition_to_leader(info, log);
        }
    }
}


impl Server {
    fn new (config: Config, tx: Sender<MainThreadMessage>)
        -> Result<Server, IoError> {
        let me = config.me;
        let log = Arc::new(Mutex::new(MemoryLog::new()));
        let state = Arc::new(Mutex::new(ServerState {
            current_state: State::Follower,
            current_term: 0,
            commit_index: 0,
            voted_for: None,
            last_leader_contact: Instant::now(),
            election_timeout: generate_election_timeout()
        }));

        // 1. Start RPC request handlers
        let append_entries_handler: Box<RpcObject> = Box::new(
            AppendEntriesHandler {state: state.clone(), log: log.clone()}
        );
        let request_vote_handler: Box<RpcObject> = Box::new(
            RequestVoteHandler {state: state.clone(), log: log.clone()}
        );
        let services = vec![
            (APPEND_ENTRIES_OPCODE, append_entries_handler),
            (REQUEST_VOTE_OPCODE, request_vote_handler)
        ];
        let mut server = RpcServer::new_with_services(services);
        try!(
            server.bind((config.me.1.ip(), config.me.1.port()))
            .and_then(|_| {
                server.repl()
            })
        );

        // 2. Start peer threads.
        let peers = config.cluster.into_iter()
            .filter(|&(id, _)| id != me.0) // filter all computers that aren't me
            .map(|(id, addr)| Peer::start((id, addr), tx.clone()))
            .collect::<Vec<PeerHandle>>();

        // 3. Construct server state object.
        let info = ServerInfo {
            peers: peers,
            me: me,
            heartbeat_timeout: config.heartbeat_timeout
        };
        
        Ok(Server {
            state: state,
            log: log,
            info: info
        })
    }

    ///
    /// Starts a new election by requesting votes from all peers.
    /// Returns the amount of time to wait before the election should time out.
    /// This function does not block to wait for the election to finish.
    /// It returns immediatly after asking each peer for a vote.
    /// It is the responsibility of the caller to wait for replies from peers and restart the
    /// election after a timeout.
    ///
    /// # Panics
    /// Panics if any other thread has panicked while holding the log lock
    ///
    fn start_election(&self, state: &mut ServerState) -> Duration {
        // transition to the candidate state
        state.transition_to_candidate(self.info.me.0);

        let (last_log_index, last_log_term) = {
            let log = self.log.lock().unwrap();
            (log.get_last_entry_index(), log.get_last_entry_term())
        };

        // tell each peer to send out RequestToVote RPCs
        let request_vote_message = RequestVoteMessage {
            term: state.current_term,
            candidate_id: self.info.me.0,
            last_log_index: last_log_index,
            last_log_term: last_log_term
        };

        for peer in &self.info.peers {
            peer.to_peer.send(PeerThreadMessage::RequestVote(request_vote_message))
                .unwrap(); // panic if the peer thread is down
        }

        state.election_timeout
    }
}

struct RequestVoteHandler {
    state: Arc<Mutex<ServerState>>,
    log: Arc<Mutex<MemoryLog>>
}

impl RpcObject for RequestVoteHandler {
    fn handle_rpc (&self, params: capnp::any_pointer::Reader, result: capnp::any_pointer::Builder) 
        ->Result<(), RpcError>
    {
        let (candidate_id, term, last_log_index, last_log_term) = try!(
            params.get_as::<request_vote::Reader>()
            .map_err(RpcError::Capnp)
            .map(|params| {
                (params.get_candidate_id(), params.get_term(), params.get_last_log_index() as usize,
                 params.get_last_log_term())
            }));
        let mut vote_granted = false;
        let current_term;
        {
            let mut state = self.state.lock().unwrap(); // panics if mutex is poisoned
            state.last_leader_contact = Instant::now();

            if term > state.current_term {
                // TODO(jason): This should happen on the main thread.
                state.transition_to_follower(term);
            }

            if state.voted_for == None || state.voted_for == Some(candidate_id) {
                let log = self.log.lock().unwrap(); // panics if mutex is poisoned
                if term == state.current_term && log.is_other_log_valid(last_log_index, last_log_term) {
                    vote_granted = true;
                    state.voted_for = Some(candidate_id);
                    // TODO(jason): This should happen on the main thread.
                    state.transition_to_follower(term);
                }
            }
            current_term = state.current_term;
        }
        let mut result_builder = result.init_as::<request_vote_reply::Builder>();
        result_builder.set_term(current_term);
        result_builder.set_vote_granted(vote_granted);
        Ok(())
    }
}

struct AppendEntriesHandler {
    state: Arc<Mutex<ServerState>>,
    log: Arc<Mutex<Log>>
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
           let prev_log_index = append_entries.get_prev_log_index() as usize;
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
           if prev_log_index == commit_index {
               // Deserialize entries from RPC.
               let entries: Vec<Entry> = append_entries.get_entries().unwrap().iter()
                   .map(Entry::from_proto).collect();
               let entries_len = entries.len();
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

///
/// Returns a new random election timeout.
/// The election timeout should be reset whenever we transition into the follower state or the
/// candidate state
///
fn generate_election_timeout() -> Duration {
    let btwn = Range::new(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX);
    let mut range = rand::thread_rng();
    Duration::from_millis(btwn.ind_sample(&mut range))
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use super::{PeerThreadMessage, PeerHandle, Server, State,
                ServerState, ServerInfo, generate_election_timeout, send_append_entries};
    use super::log::{MemoryLog, random_entry};
    use std::time::{Duration, Instant};
    use std::sync::mpsc::{channel, Receiver};
    use std::sync::{Arc, Mutex};

    const DEFAULT_HEARTBEAT_TIMEOUT_MS: u64 = 150;

    fn mock_server() -> (Receiver<PeerThreadMessage>, Server) {
        let log = Arc::new(Mutex::new(MemoryLog::new()));
        let state = Arc::new(Mutex::new(ServerState {
            current_state: State::Follower,
            current_term: 0,
            commit_index: 0,
            voted_for: None,
            last_leader_contact: Instant::now(),
            election_timeout: generate_election_timeout()
        }));
        let (tx, rx) = channel();
        let peers = vec![0, 1, 2, 3].into_iter()
            .map(|n| PeerHandle {id:n, to_peer: tx.clone(), commit_index:0})
            .collect::<Vec<PeerHandle>>();
        let server = Server {
            state: state,
            log: log,
            info: ServerInfo {
                peers: peers,
                me: (0, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)),
                heartbeat_timeout: Duration::from_millis(DEFAULT_HEARTBEAT_TIMEOUT_MS)
            }
        };
        (rx, server)
    }

    /// Makes sure peer threads receive appendEntries msesages
    /// when the log is appended to.
    #[test]
    fn test_server_send_append_entries() {
        let (rx, mut s) = mock_server();
        let vec = vec![random_entry(); 3];
        { // Append some random entries to log
          let mut log = s.log.lock().unwrap();
          log.append_entries(vec.clone());
        }

        // Send append entries to peers!
        let ref mut state = s.state.lock().unwrap();
        send_append_entries(&mut s.info, state, s.log.clone());

        // Each peer should receive a message...
        for _ in 0..s.info.peers.len() {
            match rx.recv().unwrap() {
                PeerThreadMessage::AppendEntries(entry) => {
                    assert_eq!(entry.entries.len(), vec.len());
                    assert_eq!(entry.entries, vec);
                },
                PeerThreadMessage::RequestVote(_) => panic!()
            }
        }
    }

    /// Makes sure peer threads receive requestVote messages
    /// when an election is started.
    #[test]
    fn test_server_start_election() {
        let (rx, s) = mock_server();
        let mut state = s.state.lock().unwrap();
        s.start_election(&mut state);
        // Each peer should have received a RequestVote message.
        for _ in 0..s.info.peers.len() {
            match rx.recv().unwrap() {
                PeerThreadMessage::AppendEntries(_) => panic!(),
                PeerThreadMessage::RequestVote(vote) => {
                    assert_eq!(vote.term, state.current_term);
                    assert_eq!(vote.candidate_id, s.info.me.0);
                },
            }
        }
    }
}


