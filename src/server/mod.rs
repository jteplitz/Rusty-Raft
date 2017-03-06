mod log;
use capnp;
use rand;
use capnp::serialize::OwnedSegments;
use capnp::message::Reader;
use raft_capnp::{append_entries, append_entries_reply,
                 request_vote, request_vote_reply};
use rpc::{RpcError};
use rpc::client::Rpc;
use rpc::server::{RpcObject, RpcServer};
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
    next_index: usize,
}

impl PeerHandle {
    /// Pushes a non-blocking append-entries request to this peer.
    /// Panics if associated peer background thread is somehow down.
    fn append_entries_nonblocking (&self, leader_id: u64, commit_index: usize,
                                   current_term: u64, log: Arc<Mutex<Log>>) {
        let entries = {
            log.lock().unwrap().get_entries_from(self.next_index).to_vec()
        };
        let peer_index = self.next_index;
        let last_entry = entries.get(peer_index).unwrap();
        self.to_peer.send(PeerThreadMessage::AppendEntries(AppendEntriesMessage {
            term: current_term,
            leader_id: leader_id,
            prev_log_index: peer_index,
            prev_log_term: last_entry.term,
            entries: entries.to_vec(),
            leader_commit: commit_index,
        })).unwrap(); // This failing means the peer background thread is down.
    }
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
            next_index: commit_index,
        }
    }

    ///
    /// Sends the appropriate append entries RPC to this peer.
    ///
    /// # Panics
    /// Panics if proto fails to initialize.
    ///
    // TODO(sydli): Test
    fn append_entries_blocking (&mut self, entry: AppendEntriesMessage) {
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
        Peer::construct_request_vote(&mut rpc, &vote);

        let vote_granted = rpc.send(self.addr)
            .and_then(|msg| Peer::handle_request_vote_reply(vote.term, msg))
            .unwrap_or(false);

        let reply = RequestVoteReply {
            term: vote.term,
            vote_granted: vote_granted
        };
        // Panics if the main thread has panicked or been deallocated
        self.to_main.send(MainThreadMessage::RequestVoteReply(reply)).unwrap();
    }

    ///
    /// Sets the term, candidate_id, and last_log index on the rpc from the
    /// data in the RequestVoteMessage
    ///
    fn construct_request_vote (rpc: &mut Rpc, vote: &RequestVoteMessage) {
        let mut params = rpc.get_param_builder().init_as::<request_vote::Builder>();
        params.set_term(vote.term);
        params.set_candidate_id(vote.candidate_id);
        params.set_last_log_index(vote.last_log_index as u64);
        params.set_last_log_term(vote.last_log_term);
    }

    ///
    /// Processes a request_vote_reply for the given vote_term
    /// Returms true if the peer granted their vote or false if the peer denied their vote
    ///
    /// # Errors
    /// Returns an RpcError if the msg is not a well formed request_vote_reply
    ///
    fn handle_request_vote_reply (vote_term: u64, msg: Reader<OwnedSegments>) -> Result<bool, RpcError> {
        Rpc::get_result_reader(&msg)
        .and_then(|result| {
            result.get_as::<request_vote_reply::Reader>()
                  .map_err(RpcError::Capnp)
        })
        .map(|reply_reader| {
            let reply_term = reply_reader.get_term();
            let vote_granted = reply_reader.get_vote_granted();
            (reply_term == vote_term) && vote_granted
        })
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
                PeerThreadMessage::AppendEntries(entry) => self.append_entries_blocking(entry),
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
    /// TODO: Persist term information to disk
    fn transition_to_leader(&mut self, info: &mut ServerInfo, log: Arc<Mutex<Log>>) {
        debug_assert!(matches!(self.current_state, State::Candidate{ .. }));
        self.current_state = State::Leader { last_heartbeat: Instant::now() };
        // Append dummy entry.
        { // Scope the log lock so we drop it immediately afterwards.
            log.lock().unwrap().append_entry(Entry::noop(self.current_term));
        }
        for peer in &mut info.peers {
            peer.next_index = self.commit_index;
        }
        broadcast_append_entries(info, self, log.clone());
    }

    ///
    /// Transitions into the follower state from either the candidate state or the leader state
    /// If the new term is greater than our current term this also resets
    /// our vote tracker.
    ///
    fn transition_to_follower(&mut self, new_term: u64) {
        debug_assert!(matches!(self.current_state, State::Candidate{ .. }) ||
                      matches!(self.current_state, State::Leader{ .. }));
        debug_assert!(new_term >= self.current_term);

        if new_term > self.current_term {
            self.voted_for = None;
        }
        self.current_term = new_term;
        self.current_state = State::Follower;
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
// TODO(sydli): Test
fn update_commit_index(server_info: &ServerInfo, state: &mut ServerState) {
    // Find median of all peer commit indices.
    let mut indices: Vec<usize> = server_info.peers.iter().map(|ref peer| peer.next_index.clone()).collect();
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
            // TODO(jason): Decompose and test
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
                        broadcast_append_entries(&mut server.info, &mut state, server.log.clone());
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
                handle_append_entries_reply(m, &mut server.info, state, server.log.clone()),
            MainThreadMessage::ClientAppendRequest => 
                // TODO: Rpc handler needs to already append message to log
                broadcast_append_entries(&mut server.info, state, server.log.clone()),
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
        State::Leader{ .. } => broadcast_append_entries(&mut server.info, state, server.log.clone())
    }
}

///
/// If we're leader, handle replies from peers who respond to AppendEntries.
/// If we hear about a term greater than ours, step down.
/// If we're Candidate or Follower, we drop the message.
/// 
// TODO(sydli): Test
fn handle_append_entries_reply(m: AppendEntriesReply, server_info: &mut ServerInfo, state: &mut ServerState, log: Arc<Mutex<Log>>) {
    match state.current_state {
        State::Leader{ .. } => {
            if m.term > state.current_term {
                state.transition_to_follower(m.term);
            } else if m.success {
                // On success, advance peer's index.
                server_info.get_peer_mut(m.peer.0).map(|peer| {
                    if m.commit_index > peer.next_index {
                        peer.next_index = m.commit_index + 1;
                    }
                });
                update_commit_index(server_info, state);
            } else {
                let leader_id = server_info.me.0;
                // If we failed, roll back peer index by 1 and retry
                // the append entries call.
                server_info.get_peer_mut(m.peer.0).map(|peer| {
                    peer.next_index = peer.next_index - 1;
                    peer.append_entries_nonblocking(leader_id, state.commit_index,
                                                    state.current_term, log);
                });
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
fn broadcast_append_entries(info: &mut ServerInfo, state: &mut ServerState, log: Arc<Mutex<Log>>) {
    debug_assert!(matches!(state.current_state, State::Leader{ .. }));
    for peer in &info.peers {
        // Perf: each call copies the needed |entries| from |log| to send along to the peer.
        peer.append_entries_nonblocking(info.me.0, state.commit_index,
                                        state.current_term, log.clone());
    }
    state.current_state = State::Leader { last_heartbeat: Instant::now() };
}

///
/// Handles replies to RequestVote Rpc
///
// TODO(jason): Test
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

// TODO(jason): Test
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


// TODO(sydli): Test
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
    use capnp::{message, serialize_packed};
    use capnp::serialize::OwnedSegments;
    use std::io::BufReader;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use super::*;
    use super::{PeerThreadMessage, ServerInfo, broadcast_append_entries, ServerState,
                State, generate_election_timeout, PeerHandle, Peer, RequestVoteMessage};
    use super::super::rpc::client::*;
    use super::super::raft_capnp::{request_vote, request_vote_reply, append_entries_reply};
    use super::super::rpc_capnp::rpc_response;
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
            .map(|n| PeerHandle {id:n, to_peer: tx.clone(), next_index:0})
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
    fn server_sends_append_entries() {
        let (rx, mut s) = mock_server();
        let vec = vec![random_entry(); 3];
        { // Append some random entries to log
          let mut log = s.log.lock().unwrap();
          log.append_entries(vec.clone());
        }

        // Send append entries to peers!
        let ref mut state = s.state.lock().unwrap();
        state.current_state = State::Leader { last_heartbeat: Instant::now() };
        broadcast_append_entries(&mut s.info, state, s.log.clone());

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
    fn server_starts_election() {
        let (rx, s) = mock_server();
        let mut state = s.state.lock().unwrap();
        s.start_election(&mut state);
        assert!(matches!(state.current_state, State::Candidate{ .. }));
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

    #[test]
    fn peer_constructs_valid_request_vote() {
        const TERM: u64 = 13;
        const CANDIDATE_ID: u64 = 6;
        const LAST_LOG_INDEX: u64 = 78;
        const LAST_LOG_TERM: u64 = 5;
        let mut rpc = Rpc::new(1);
        let vote = RequestVoteMessage {
            term: TERM,
            candidate_id: CANDIDATE_ID,
            last_log_index: LAST_LOG_INDEX as usize,
            last_log_term: LAST_LOG_TERM
        };

        Peer::construct_request_vote(&mut rpc, &vote);
        let param_reader = rpc.get_param_builder().as_reader().get_as::<request_vote::Reader>().unwrap();
        assert_eq!(param_reader.get_term(), TERM);
        assert_eq!(param_reader.get_candidate_id(), CANDIDATE_ID);
        assert_eq!(param_reader.get_last_log_index(), LAST_LOG_INDEX);
        assert_eq!(param_reader.get_last_log_term(), LAST_LOG_TERM);
    }

    fn get_message_reader<A> (msg: &message::Builder<A>) -> message::Reader<OwnedSegments> 
        where A: message::Allocator
    {
        let mut message_buffer = Vec::new();
        serialize_packed::write_message(&mut message_buffer, &msg).unwrap();

        let mut buf_reader = BufReader::new(&message_buffer[..]);
        serialize_packed::read_message(&mut buf_reader, message::ReaderOptions::new()).unwrap()
    }

    #[test]
    fn peer_vote_reply_handles_vote_granted() {
        const TERM: u64 = 54;
        let mut builder = message::Builder::new_default();
        construct_request_vote_reply(&mut builder, TERM, true);

        let reader = get_message_reader(&builder);
        assert!(Peer::handle_request_vote_reply(TERM, reader).unwrap());
    }

    #[test]
    fn peer_vote_reply_handles_incorrect_term() {
        const TERM: u64 = 26;
        let mut builder = message::Builder::new_default();
        construct_request_vote_reply(&mut builder, TERM - 1, true);

        let reader = get_message_reader(&builder);
        assert!(!Peer::handle_request_vote_reply(TERM, reader).unwrap());
    }

    #[test]
    fn peer_vote_reply_handles_vote_rejected() {
        const TERM: u64 = 14;
        let mut builder = message::Builder::new_default();
        construct_request_vote_reply(&mut builder, TERM, false);

        let reader = get_message_reader(&builder);
        assert!(!Peer::handle_request_vote_reply(TERM, reader).unwrap());
    }

    #[test]
    // TODO: Determine what level of error checking we want on the messages
    // I have not found consistent error behavior. Even out of bounds seems to proceed
    // without error on travis...
    fn peer_vote_reply_handles_malformed_vote_replies() {
        /*const TERM: u64 = 14;
        let mut builder = message::Builder::new_default();
        construct_append_entries_reply(&mut builder, TERM, true);

        let reader = get_message_reader(&builder);
        let err = Peer::handle_request_vote_reply(TERM, reader).unwrap_err();
        assert!(matches!(err, RpcError::Capnp(_)));*/
    }

    #[test]
    fn peer_vote_reply_handles_malformed_rpc_replies() {
        // TODO(jason): Figure out why this test fails on travis
        /*const TERM: u64 = 19;
        let builder = message::Builder::new_default();
        let reader = get_message_reader(&builder);
        let err = Peer::handle_request_vote_reply(TERM, reader).unwrap_err();
        assert!(matches!(err, RpcError::Capnp(_)));*/
    }

    /// Constructs a valid rpc_response with the given information contained in an
    /// append_entries_reply inside the provided msg buffer.
    fn construct_append_entries_reply<A> (msg: &mut message::Builder<A>, term: u64, success: bool)
        where A: message::Allocator
    {
        let response_builder = msg.init_root::<rpc_response::Builder>();
        let mut reply_builder = response_builder.get_result().init_as::<append_entries_reply::Builder>();
        reply_builder.set_term(term);
        reply_builder.set_success(success);
    }

    /// Constructs a valid rpc_response with the given information contained in a request_vote_reply
    /// inside the provided msg buffer.
    fn construct_request_vote_reply<A> (msg: &mut message::Builder<A>, term: u64, vote_granted: bool) 
        where A: message::Allocator 
    {
        let response_builder = msg.init_root::<rpc_response::Builder>();
        let mut vote_reply_builder = response_builder.get_result().init_as::<request_vote_reply::Builder>();
        vote_reply_builder.set_term(term);
        vote_reply_builder.set_vote_granted(vote_granted);
    }

    // TODO: Come up with a consistent way to structure modules and unit tests 
    mod server_state {
        use super::mock_server;
        use super::super::{State, PeerThreadMessage};
        use super::super::log::{Op};

        #[test]
        fn transition_to_candidate_normal() {
            const OUR_ID: u64 = 5;
            let (_, s) = mock_server();
            let mut state = s.state.lock().unwrap();
            assert_eq!(state.current_term, 0);
            assert_eq!(state.voted_for, None);
            assert!(matches!(state.current_state, State::Follower));

            state.transition_to_candidate(OUR_ID);
            match state.current_state {
                State::Candidate{num_votes, ..} => {
                    assert_eq!(state.current_term, 1);
                    assert_eq!(state.voted_for, Some(OUR_ID));
                    assert_eq!(num_votes, 1);
                },
                _ => panic!("Transition to candidate did not enter the candidate state.")
            }
        }

        #[test]
        fn transition_to_leader_normal() {
            const OUR_ID: u64 = 1;
            let (rx, mut s) = mock_server();
            let mut state = s.state.lock().unwrap();
            // we must be a candidate before we can become a leader
            state.transition_to_candidate(OUR_ID);
            state.transition_to_leader(&mut s.info, s.log.clone());
            assert!(matches!(state.current_state, State::Leader{..}));

            // ensure that we appended a dummy log entry
            let log = s.log.lock().unwrap();
            assert_eq!(log.get_last_entry_index(), 1);
            let entry = log.get_entry(1).unwrap();
            assert!(matches!(entry.op, Op::Noop));

            // ensure that the dummy entry was broadcasted properly
            for _ in 0..s.info.peers.len() {
                match rx.recv().unwrap() {
                    PeerThreadMessage::AppendEntries(msg) => {
                        assert_eq!(msg.entries.len(), 1);
                        assert_eq!(msg.entries[0], *entry);
                    },
                    _=> panic!("Incorrect message type sent in response to transition to leader")
                }
            }
        }

        #[test]
        fn transition_to_follower_normal() {
            const OUR_ID: u64 = 1;
            let (_, s) = mock_server();
            let mut state = s.state.lock().unwrap();
            state.transition_to_candidate(OUR_ID);
            assert_eq!(state.current_term, 1);
            assert_eq!(state.voted_for, Some(OUR_ID));
            state.transition_to_follower(1);
            assert_eq!(state.current_term, 1);
            assert_eq!(state.voted_for, Some(OUR_ID));
            assert!(matches!(state.current_state, State::Follower));
        }

        #[test]
        fn transition_to_follower_wipes_vote_if_new_term() {
            const OUR_ID: u64 = 1;
            const NEW_TERM: u64 = 2;
            let (_, s) = mock_server();
            let mut state = s.state.lock().unwrap();
            state.transition_to_candidate(OUR_ID);
            assert_eq!(state.current_term, 1);
            assert_eq!(state.voted_for, Some(OUR_ID));
            state.transition_to_follower(NEW_TERM);
            assert_eq!(state.current_term, NEW_TERM);
            assert_eq!(state.voted_for, None);
            assert!(matches!(state.current_state, State::Follower));
        }
    }
}

