mod log;
mod peer;
mod constants;
use capnp;
use rand;
use raft_capnp::{append_entries, append_entries_reply,
                 request_vote, request_vote_reply,
                 client_request, client_request_reply, Op as ProtoOp};
use rpc::{RpcError};
use rpc::server::{RpcObject, RpcServer};
use state_machine::{ExactlyOnceStateMachine, SessionInfo, Config, RaftError};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use std::thread;
use std::thread::JoinHandle;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::mpsc::{channel, Sender, Receiver, RecvTimeoutError};
use std::mem;

use self::log::{Log, MemoryLog, Entry, Op};
use self::peer::{Peer, PeerHandle, PeerThreadMessage, RequestVoteMessage};
use std::io::Error as IoError;
use rand::distributions::{IndependentSample, Range};

pub use self::constants::CLIENT_REQUEST_OPCODE;

// States that each machine can be in!
#[derive(Debug, PartialEq, Clone, Copy)]
enum State {
    Candidate { num_votes: usize, start_time: Instant },
    Leader { last_heartbeat: Instant },
    Follower,
}

// TODO: Move messages into seperate file

///
/// Messages that can be sent to the main thread.
/// *Reply messages encapsulate replies from peer machines, and
/// ClientAppendRequest is a message sent by the client thread.
pub struct AppendEntriesReply {
    term: u64,
    commit_index: usize,
    peer: (u64, SocketAddr),
    success: bool,
}

#[derive(PartialEq, Clone, Copy)]
pub struct RequestVoteReply {
    term: u64,
    vote_granted: bool,
}

pub enum MainThreadMessage {
    AppendEntriesReply (AppendEntriesReply),
    RequestVoteReply (RequestVoteReply),
    ClientAppendRequest,
    Shutdown
}

///
/// Types of messages to be sent to the state machine thread.
///
enum StateMachineMessage {
    Command (usize),
    Query { buffer: Vec<u8>, response_channel: Sender<Result<Vec<u8>, RaftError>> },
    Shutdown
}

///
/// Applies entries (next_index, to_commit) exclusive, exclusive from |log|
/// to the |state_machine|. Returns the new committed index.
fn apply_commands(next_index: usize, to_commit: usize,
                  log: Arc<Mutex<Log>>,
                  state_machine: &Box<ExactlyOnceStateMachine>)
        -> usize {
    if to_commit < next_index { return next_index; }
    let to_apply = { log.lock().unwrap().get_entries_from(next_index - 1)
        [.. (to_commit - next_index + 1) ].to_vec() };
    for entry in to_apply.into_iter() {
        match entry.op {
            Op::Write{data, session} => {
                state_machine.command(&data, session);
            },
            Op::OpenSession => {state_machine.new_session();},
            Op::Read(_) | Op::Noop | Op::Unknown => {},
        };
    }
    to_commit + 1
}

// TODO Move StateMachine logic to seperate file
struct StateMachineHandle {
    thread: Option<JoinHandle<()>>,
    pub tx: Sender<StateMachineMessage>
}

impl Drop for StateMachineHandle {
    /// Signals the state machine to shutdown and blocks until it does.
    ///
    /// #Panics
    /// Panics if the state machine has panicked
    fn drop (&mut self) {
        let thread = mem::replace(&mut self.thread, None);
        match thread {
            Some(t) => {
                self.tx.send(StateMachineMessage::Shutdown).unwrap();
                t.join().unwrap();
            },
            None => {/* Nothing to drop*/}
        }
    }
}

///
/// Starts thread responsible for performing operations on state machine.
/// Loops waiting on a channel for operations to perform.
///
/// |Command| messages first advances an internal index pointer, then performs
/// StateMachine::write on the log entry's command buffer at that index if it
/// is a Write operation. This index is initialized at |start_index|.
///
/// |Query| messages perform StateMachine::read on the |query_buffer|, and
/// sends the result over |response_channel|.
///
/// Returns a Sender handle to this thread.
///
fn state_machine_thread (log: Arc<Mutex<Log>>,
                         start_index: usize,
                         state_machine: Box<ExactlyOnceStateMachine>,
                         ) -> StateMachineHandle {
    let(to_state_machine, from_main) = channel();
    let t = thread::spawn(move || {
        let mut next_index = start_index + 1;
        loop {
            match from_main.recv().unwrap() {
                StateMachineMessage::Command(commit_index) => {
                    next_index = apply_commands(next_index, commit_index, log.clone(), &state_machine);
                },
                StateMachineMessage::Query { buffer, response_channel } => {
                    response_channel.send(state_machine.query(&buffer)).unwrap();
                },
                // TODO: Allow state machines to provide custom shutdown logic?
                StateMachineMessage::Shutdown => break
            }
        }
    });
    StateMachineHandle {thread: Some(t), tx: to_state_machine}
}

/// Store's the state that the server is currently in along with the current_term
/// and current_id. These fields should all share a lock.
#[derive(Debug, PartialEq, Clone, Copy)]
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
    fn transition_to_candidate(&mut self, my_id: u64, cv: &Condvar) {
        debug_assert!(self.current_state == State::Follower ||
                      matches!(self.current_state, State::Candidate { .. }));
        self.current_state = State::Candidate { start_time: Instant::now(), num_votes: 1 }; // we always start with 1 vote from ourselves
        self.current_term += 1;
        self.voted_for = Some(my_id); // vote for ourselves
        self.election_timeout = generate_election_timeout();
        cv.notify_all();
    }

    ///
    /// Transitions into the leader state from the candidate state.
    ///
    /// TODO: Persist term information to disk
    fn transition_to_leader(&mut self, info: &mut ServerInfo, log: Arc<Mutex<Log>>, cv: &Condvar) {
        debug_assert!(matches!(self.current_state, State::Candidate{ .. }));
        self.current_state = State::Leader { last_heartbeat: Instant::now() };
        // Append dummy entry.
        { // Scope the log lock so we drop it immediately afterwards.
            log.lock().unwrap().append_entry(Entry::noop(self.current_term));
        }
        for peer in &mut info.peers {
            peer.next_index = self.commit_index + 1;
        }
        self.commit_index = self.commit_index + 1;
        broadcast_append_entries(info, self, log.clone());
        cv.notify_all();
    }

    ///
    /// Transitions into the follower state from either the candidate state or the leader state
    /// If the new term is greater than our current term this also resets
    /// our vote tracker.
    ///
    fn transition_to_follower(&mut self, new_term: u64, cv: &Condvar) {
        debug_assert!(new_term >= self.current_term);

        if new_term > self.current_term {
            self.voted_for = None;
        } else {
            debug_assert!(matches!(self.current_state, State::Candidate{ .. }) ||
                          matches!(self.current_state, State::Leader{ .. }));
        }
        self.current_term = new_term;
        self.current_state = State::Follower;
        self.election_timeout = generate_election_timeout();
        cv.notify_all();
        // TODO: We need to stop the peers from continuing to send AppendEntries here.
    }
}

// TODO: RW locks?
// NB: State lock should never be acquired while holding the log lock
pub struct Server {
    // TODO: Rename properties?
    state: Arc<(Mutex<ServerState>, Condvar)>,
    log: Arc<Mutex<Log>>,
    info: ServerInfo,
}

struct ServerInfo {
    peers: Vec<PeerHandle>,
    state_machine: StateMachineHandle,
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
fn update_commit_index(server_info: &ServerInfo, state: &mut ServerState, cv: &Condvar) {
    // Find median of all peer commit indices.
    let mut indices: Vec<usize> = server_info.peers.iter().map(|ref peer| peer.match_index).collect();
    indices.sort();
    let new_index = *indices.get( indices.len() / 2 ).unwrap();
    // Set new commit index if it's higher!
    if new_index <= state.commit_index { return; }
    state.commit_index = new_index;
    server_info.state_machine.tx.send(StateMachineMessage::Command(state.commit_index)).unwrap();
    cv.notify_all();
}

pub struct ServerHandle {
    tx: Sender<MainThreadMessage>,
    thread: Option<JoinHandle<()>>,
    addr: SocketAddr,
    rpc_server: Option<RpcServer>
}

impl ServerHandle {
    pub fn shutdown(self) {
    }

    /// 
    /// Returns the local address of the given server.
    /// This is useful if you started a server with port 0 and want to know which port the OS
    /// assigned to the server.
    ///
    pub fn get_local_addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for ServerHandle {
    /// Gracefully shuts down the raft server. Blocking until all incoming connections
    /// have been dealt with.
    ///
    /// This should not be called from a thread that would deal with an incoming conncetion or else
    /// it could deadlock
    ///
    /// #Panics
    /// Panics if the main server thread has panicked
    fn drop (&mut self) {
        let thread = mem::replace(&mut self.thread, None);
        match thread {
            Some(t) => {
                {
                    // drop the rpc server (if it exists)
                    let rpc_server = mem::replace(&mut self.rpc_server, None);
                }
                // send the shutdown message to the main thread
                self.tx.send(MainThreadMessage::Shutdown).unwrap();
                // join the main thread
                t.join().unwrap();
            },
            None => {/* Nothing to shutdown*/}
        };
    }
}

///
/// Starts up a new raft server with the given config.
/// This is mostly just a bootstrapper for now. It probably won't end up in the public API
/// As of right now this function does not return. It just runs the state machine as long as it's
/// in a live thread.
///
pub fn start_server<F> (config: Config, load_state_machine: F) -> Result<ServerHandle, IoError>
    where F: FnOnce() -> Box<ExactlyOnceStateMachine> {
    let (tx, rx) = channel();
    let tx_clone = tx.clone();
    let (server, rpc_server) = try!(Server::new(config, tx, load_state_machine()));
    let addr = try!(rpc_server.get_local_addr());

    // start the server
    let join_handle = server.repl(rx);

    Ok(ServerHandle {
        tx: tx_clone,
        thread: Some(join_handle),
        addr: addr,
        rpc_server: Some(rpc_server)
    })
}

///
/// If we're leader, handle replies from peers who respond to AppendEntries.
/// If we hear about a term greater than ours, step down.
/// If we're Candidate or Follower, we drop the message.
/// 
// TODO(sydli): Test
fn handle_append_entries_reply(m: AppendEntriesReply, server_info: &mut ServerInfo, state: &mut ServerState, state_condition: &Condvar, log: Arc<Mutex<Log>>) {
    match state.current_state {
        State::Leader{ .. } => (),
        State::Candidate{ .. } | State::Follower => return, // drop it like it's hot
    }
    if m.term > state.current_term {
        state.transition_to_follower(m.term, state_condition);
    } else if m.success {
        // On success, advance peer's index.
        server_info.get_peer_mut(m.peer.0).map(|peer| {
            debug_assert!(m.commit_index >= peer.next_index - 1);
            peer.next_index = m.commit_index + 1;
            peer.match_index = m.commit_index;
        });
        update_commit_index(server_info, state, state_condition);
    } else {
        let leader_id = server_info.me.0;
        // If we failed, roll back peer index by 1 (if we can) and retry
        // the append entries call.
        server_info.get_peer_mut(m.peer.0).map(|peer| {
            if peer.next_index > 1 {
                // can we make sure match_index never exceeds peer_index?
                peer.next_index = peer.next_index - 1;
            }
            peer.append_entries_nonblocking(leader_id,
                                            state.commit_index,
                                            state.current_term, log);
        });
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
fn handle_request_vote_reply(reply: RequestVoteReply, info: &mut ServerInfo,
                             state: &mut ServerState, state_condition: &Condvar,
                             log: Arc<Mutex<Log>>) {
    // Since we can't have two mutable borrows on |state| at once we gotta
    // update votes & do the transition check in separate scopes.
    if reply.term == state.current_term && reply.vote_granted {
        if let State::Candidate{ref mut num_votes, ..} = state.current_state {
            *num_votes += 1;
        }
    }
    if let State::Candidate{num_votes, ..} = state.current_state {
        if num_votes > (info.peers.len() + 1) / 2 {
            // Woo! We won the election
            state.transition_to_leader(info, log, state_condition);
            println!("Server {}: Became leader for term {} with {} votes out of {} peers", info.me.0, state.current_term, num_votes,
                     info.peers.len());
        }
    }
}

impl Server {
    fn new (config: Config, tx: Sender<MainThreadMessage>, state_machine: Box<ExactlyOnceStateMachine>)
        -> Result<(Server, RpcServer), IoError> {
        let me = config.me;
        let log = Arc::new(Mutex::new(MemoryLog::new()));
        let state = Arc::new((Mutex::new(ServerState {
            current_state: State::Follower,
            current_term: 0,
            commit_index: 0,
            voted_for: None,
            last_leader_contact: Instant::now(),
            election_timeout: generate_election_timeout(),
        }), Condvar::new()));

        // 1a. Start state machine thread.
        let state_machine_handle = state_machine_thread(log.clone(), 0, state_machine);
        let to_state_machine_locked = 
            Arc::new(Mutex::new(state_machine_handle.tx.clone()));

        // 1. Start RPC request handlers
        let to_main_locked = Arc::new(Mutex::new(tx.clone()));
        let append_entries_handler: Box<RpcObject> = Box::new(
            AppendEntriesHandler {state: state.clone(), log: log.clone(),
                                  to_state_machine: to_state_machine_locked.clone() }
        );
        let request_vote_handler: Box<RpcObject> = Box::new(
            RequestVoteHandler {state: state.clone(), log: log.clone()}
        );
        let client_request_handler: Box<RpcObject> = Box::new(
            ClientRequestHandler {state: state.clone(), log: log.clone(),
                                  to_main: to_main_locked.clone(),
                                  to_state_machine: to_state_machine_locked.clone() }
        );
        let services = vec![
            (constants::APPEND_ENTRIES_OPCODE, append_entries_handler),
            (constants::REQUEST_VOTE_OPCODE, request_vote_handler),
            (constants::CLIENT_REQUEST_OPCODE, client_request_handler)
        ];
        let mut rpc_server = RpcServer::new_with_services(services);
        try!(
            rpc_server.bind(me.1)
            .and_then(|_| {
                rpc_server.repl()
            })
        );

        // 2. Start peer threads.
        let peers = config.cluster.into_iter()
            .filter(|&(id, _)| id != me.0) // filter all computers that aren't me
            .map(|(id, addr)| Peer::start((id, addr), tx.clone()))
            .collect::<Vec<PeerHandle>>();

        // 3. Construct server state object.
        
        // safe to unwrap here because we should only get this far
        // if the server is bound.
        let bound_address = rpc_server.get_local_addr().unwrap();
        let info = ServerInfo {
            peers: peers,
            me: (me.0, bound_address),
            heartbeat_timeout: config.heartbeat_timeout,
            state_machine: state_machine_handle 
        };

        Ok((Server {
            state: state,
            log: log,
            info: info,
        }, rpc_server))
    }

    /// Starts running the raft consensus algorithim in a background thread.
    /// Consumes the server in the process. All future communication with this
    /// background thread should be from a sender that is linked to the passed in receiver
    ///
    /// #Panics
    /// Panics if the OS fails to spawn a thread
    fn repl(mut self, rx: Receiver<MainThreadMessage>) -> JoinHandle<()> {
        thread::spawn(move || {
            // NB: This thread handles all state changes EXCEPT for those that move us back into the
            // follower state from either the candidate or leader state. Those are both handled in the
            // AppendEntriesHandler

            loop {
                let current_timeout;
                { // state lock scope
                    // TODO(jason): Decompose and test
                    let &(ref state_ref, ref cvar) = &*self.state;
                    let state = state_ref.lock().unwrap();

                    match state.current_state {
                        State::Follower => {
                            let now = Instant::now();
                            current_timeout = state.election_timeout.checked_sub(now.duration_since(state.last_leader_contact));
                        }
                        State::Candidate{start_time, ..} => {
                            let time_since_election = Instant::now() - start_time;

                            current_timeout = state.election_timeout.checked_sub(time_since_election);
                        },
                        State::Leader{last_heartbeat} => {
                            let heartbeat_timeout = self.info.heartbeat_timeout;
                            // TODO: Should last_heartbeat be in the leader state?
                            let since_last_heartbeat = Instant::now()
                                                           .duration_since(last_heartbeat);
                            current_timeout = heartbeat_timeout.checked_sub(since_last_heartbeat);
                        },
                    } // end match server.state
                } // release state lock

                // Acquire state lock
                let message = current_timeout
                .ok_or(RecvTimeoutError::Timeout)
                .and_then(|timeout| {
                    rx.recv_timeout(timeout)
                });

                match message {
                    Ok(m) => {
                        let &(ref state_ref, ref cvar) = &*self.state;
                        let ref mut state = * state_ref.lock().unwrap();
                        // TODO(Jason): Add a shutdown message that the client can send
                        // to gracefully shutdown the server
                        match m {
                            MainThreadMessage::AppendEntriesReply(m) => {
                                handle_append_entries_reply(m, &mut self.info, state, cvar, self.log.clone());
                            },
                            MainThreadMessage::ClientAppendRequest => {
                                broadcast_append_entries(&mut self.info, state, self.log.clone());
                            },
                            MainThreadMessage::RequestVoteReply(m) =>  {
                                handle_request_vote_reply(m, &mut self.info, state, cvar, self.log.clone());
                            },
                            MainThreadMessage::Shutdown => {
                                break;
                            }
                        };
                    },
                    Err(_) => {
                        self.handle_timeout();
                        continue;
                    }
                }
                
            } // end loop
            
            // shutdown the peers by dropping their handles
            self.info.peers.clear();
        })
    }

    ///
    /// Handles timeouts on the main thread.
    /// Timeouts are handled differently based on our current state.
    /// If we're a follower or candidate, we check if we've recieved 
    /// anything from the leader since we went to sleep. 
    /// If not then we start a new election
    /// If we're the leader we send a hearbeat
    ///
    /// # Panics
    /// Panics if any of the peer threads have panicked.
    ///
    fn handle_timeout(&mut self) {
        let (ref state_ref, ref cvar) = *self.state;
        let ref mut state = * state_ref.lock().unwrap();

        match state.current_state {
            State::Follower | State::Candidate{ .. } => {
                let now = Instant::now();
                if now.duration_since(state.last_leader_contact) > state.election_timeout {
                    self.start_election(state, cvar);
                }
            },
            State::Leader{ .. } => {
                broadcast_append_entries(&mut self.info, state, self.log.clone());
            }
        }
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
    fn start_election(&self, state: &mut ServerState, state_condition: &Condvar) -> Duration {
        println!("Server {} in state {:?}: Starting election for term {}. Previously voted for {:?}", self.info.me.0, state.current_state,
                 state.current_term + 1, state.voted_for);
        // transition to the candidate state
        state.transition_to_candidate(self.info.me.0, state_condition);

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
    state: Arc<(Mutex<ServerState>, Condvar)>,
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
            let &(ref state_ref, ref cvar) = &*self.state;
            let mut state = state_ref.lock().unwrap(); // panics if mutex is poisoned
            state.last_leader_contact = Instant::now();

            if term > state.current_term {
                // transition to a follower of the new term
                // TODO(jason): This should happen on the main thread.
                state.transition_to_follower(term, &cvar);
            }

            if state.voted_for == None || state.voted_for == Some(candidate_id) {
                let log = self.log.lock().unwrap(); // panics if mutex is poisoned
                if term == state.current_term && log.is_other_log_valid(last_log_index, last_log_term) {
                    vote_granted = true;
                    state.voted_for = Some(candidate_id);
                    // TODO(jason): This should happen on the main thread.
                    if !matches!(state.current_state, State::Follower{ .. }) {
                        // step down
                        state.transition_to_follower(term, &cvar);
                    }
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
    state: Arc<(Mutex<ServerState>, Condvar)>,
    log: Arc<Mutex<Log>>,
    to_state_machine: Arc<Mutex<Sender<StateMachineMessage>>>,
}

impl AppendEntriesHandler {
    fn handle_message(&self, message: append_entries::Reader, reply: &mut append_entries_reply::Builder) {
        let &(ref state_ref, ref cvar) = &*(self.state);
        let mut state = state_ref.lock().unwrap();
        let current_term = state.current_term;
        reply.set_success(false);
        reply.set_term(current_term);

        // Check: If our term doesn't match the message's term...
        if message.get_term() < current_term { return; }
        if message.get_term() > current_term || 
            (message.get_term() == current_term && matches!(state.current_state, State::Candidate{..})) {
            // Become follower for the higher term
            state.transition_to_follower(message.get_term(), cvar);
            reply.set_term(current_term);
        }

        debug_assert!(message.get_term() == state.current_term);
        // Reset election timer for this term.
        state.last_leader_contact = Instant::now();
        // Check: (prev_log_term, prev_log_index) exists in our log
        let prev_log_index = message.get_prev_log_index() as usize;
        let (last_log_index, prev_log_entry_term) = {
            let log = self.log.lock().unwrap();
            (log.get_last_entry_index(),
             log.get_entry(prev_log_index).map(|x| x.term).unwrap_or(0))
        };
        // Does (term, index) exist in our log?
        if prev_log_index > last_log_index ||
           prev_log_entry_term != message.get_prev_log_term() { return; }
        // Append all the entries to our log.
        let entries: Vec<Entry> = message.get_entries().unwrap().iter()
            .map(Entry::from_proto).collect();
        let commit_index = { // Append entries to log.
            let mut log = self.log.lock().unwrap();
            log.roll_back(prev_log_index);
            log.append_entries(entries);
            log.get_last_entry_index()
        };
        // March forward the commit index.
        if commit_index > state.commit_index {
            state.commit_index = commit_index;
            self.to_state_machine.lock().unwrap()
                .send(StateMachineMessage::Command(state.commit_index)).unwrap();
        }
        reply.set_success(true);
    }
}


impl RpcObject for AppendEntriesHandler {
    fn handle_rpc (&self, params: capnp::any_pointer::Reader, result: capnp::any_pointer::Builder) 
        -> Result<(), RpcError>
    {
        let mut reply = result.init_as::<append_entries_reply::Builder>();
        params.get_as::<append_entries::Reader>()
            .map(|append_entries| self.handle_message(append_entries, &mut reply))
            .map_err(RpcError::Capnp)
    }
}

///
/// Returns a new random election timeout.
/// The election timeout should be reset whenever we transition into the follower state or the
/// candidate state
///
fn generate_election_timeout() -> Duration {
    let btwn = Range::new(constants::ELECTION_TIMEOUT_MIN, constants::ELECTION_TIMEOUT_MAX);
    let mut range = rand::thread_rng();
    Duration::from_millis(btwn.ind_sample(&mut range))
}


///
/// Client read. Blocks until most recent write is properly committed to the log,
/// then returns result from client state machine query.
///
fn client_read_blocking (query: &[u8],
                         to_state_machine: Arc<Mutex<Sender<StateMachineMessage>>>
                         ) -> Result<Vec<u8>, RaftError> {
    let (to_me, from_sm) = channel();
    to_state_machine.lock().unwrap().send(
        StateMachineMessage::Query {
            buffer: query.to_vec(),
            response_channel: to_me,
        }).unwrap();
    from_sm.recv().unwrap()
}

///
/// Wait for commit index to reach |index| or stop when term has changed.
/// Returns true if commit succeeds in this |term|; false if term has changed.
///
fn wait_for_commit(term: u64, index: usize, state: Arc<(Mutex<ServerState>, Condvar)>)
    -> bool {
    let &(ref state_ref, ref cvar) = &*state;
    let mut state = state_ref.lock().unwrap();
    while state.commit_index < index && state.current_term == term {
        state = cvar.wait(state).unwrap();
    }
    state.commit_index >= index && state.current_term == term
}

///
/// Client write. Blocks until client write is properly committed to the log.
/// Returns true if client write succeeds; false if term changes and the write failed.
///
fn client_write_blocking (log: Arc<Mutex<Log>>,
                          op: Op,
                          state: Arc<(Mutex<ServerState>, Condvar)>,
                          to_main: Arc<Mutex<Sender<MainThreadMessage>>>)
                          -> Result<(), RaftError> {
    // TODO (sydli): it may not be 100% safe to drop the state lock here.
    let (current_state, current_term) = {
        let state = state.0.lock().unwrap();
        (state.current_state, state.current_term)
    };
    if !matches!(current_state, State::Leader { .. }) { 
        return Err(RaftError::NotLeader(None));
    }
    let index = { // Scope the log lock so we drop it immediately afterwards.
        let mut log = log.lock().unwrap();
        log.append_entry(Entry {index: 0, term: current_term, op: op}).get_last_entry_index()
    };
    let unwrapped_to_main = to_main.lock().unwrap();
    unwrapped_to_main.send(MainThreadMessage::ClientAppendRequest).unwrap();
    // wait for commit index to update or term to change
    if wait_for_commit(current_term, index, state.clone()) {
        Ok(())
    } else {
        Err(RaftError::NotLeader(None))
    }
}

struct ClientRequestHandler {
    state: Arc<(Mutex<ServerState>, Condvar)>,
    log: Arc<Mutex<Log>>,
    to_main: Arc<Mutex<Sender<MainThreadMessage>>>,
    to_state_machine: Arc<Mutex<Sender<StateMachineMessage>>>,
}
impl ClientRequestHandler {
    fn client_write_blocking(&self, op: Op)
        -> Result<(), RaftError>
    {
        client_write_blocking(
            self.log.clone(),
            op,
            self.state.clone(),
            self.to_main.clone())
    }
}

impl RpcObject for ClientRequestHandler {
    fn handle_rpc (&self, params: capnp::any_pointer::Reader,
                   result: capnp::any_pointer::Builder) -> Result<(), RpcError> {
        params.get_as::<client_request::Reader>().map(|client_request| {
            let op_result = match client_request.get_op().unwrap() {
                ProtoOp::Write => self.client_write_blocking(
                        Op::Write {
                            data: client_request.get_data().unwrap().to_vec(),
                            session: SessionInfo::from_proto(
                                client_request.get_session().unwrap()),
                        }).map(|_| vec![]),
                ProtoOp::OpenSession => self.client_write_blocking(Op::OpenSession)
                                            .map(|_| vec![]),
                ProtoOp::Read => client_read_blocking(
                        client_request.get_data().unwrap(),
                        self.to_state_machine.clone()),
                ProtoOp::Noop | ProtoOp::Unknown => Ok(vec![]), 
            };
            let mut reply = result.init_as::<client_request_reply::Builder>();
            match op_result {
                Ok(data) => {
                    reply.set_success(true);
                    reply.set_data(&data);
                }
                Err(_) => reply.set_success(false),

            }
        })
        .map_err(RpcError::Capnp)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use super::*;
    use super::peer::{PeerThreadMessage, PeerHandle};
    use super::{ServerInfo, broadcast_append_entries, ServerState,
                State, generate_election_timeout, update_commit_index,
                StateMachineMessage,
                handle_request_vote_reply};
    use super::log::{MemoryLog, random_entries_with_term};
    use std::time::{Duration, Instant};
    use std::sync::mpsc::{channel, Receiver};
    use std::sync::{Arc, Mutex, Condvar};

    const DEFAULT_HEARTBEAT_TIMEOUT_MS: u64 = 150;

    fn mock_server(num_peers: u64) -> (Receiver<PeerThreadMessage>, Receiver<StateMachineMessage>, Server) {
        let log = Arc::new(Mutex::new(MemoryLog::new()));
        let state = Arc::new((Mutex::new(ServerState {
            current_state: State::Follower,
            current_term: 0,
            commit_index: 0,
            voted_for: None,
            last_leader_contact: Instant::now(),
            election_timeout: generate_election_timeout()
        }), Condvar::new()));
        let (tx, rx) = channel();
        let (tx1, rx1) = channel();
        let peers = (0 .. num_peers)
            .map(|n| PeerHandle {id: n, to_peer: tx.clone(),
                                 next_index: 1, match_index: 0, thread: None})
            .collect::<Vec<PeerHandle>>();
        let server = Server {
            state: state,
            log: log,
            info: ServerInfo {
                peers: peers,
                me: (0, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)),
                heartbeat_timeout: Duration::from_millis(DEFAULT_HEARTBEAT_TIMEOUT_MS),
                state_machine: StateMachineHandle {tx: tx1, thread: None}
            },
        };
        (rx, rx1, server)
    }

    /// Makes sure peer threads receive appendEntries msesages
    /// when the log is appended to.
    #[test]
    fn server_sends_append_entries() {
        const NUM_PEERS: u64 = 4;
        const TERM: u64 = 5;
        const NUM_ENTRIES: usize = 3;
        let (rx, _, mut s) = mock_server(NUM_PEERS);
        let vec = random_entries_with_term(NUM_ENTRIES, TERM);
        let commit_index = { // Append some random entries to log
          let mut log = s.log.lock().unwrap();
          log.append_entries(vec.clone());
          log.get_last_entry_index()
        } - 1;
        // Send append entries to peers!
        let ref mut state = s.state.0.lock().unwrap();
        state.commit_index = commit_index + 1;
        state.current_state = State::Leader { last_heartbeat: Instant::now() };
        broadcast_append_entries(&mut s.info, state, s.log.clone());

        // Each peer should receive a message...
        for _ in 0..s.info.peers.len() {
            match rx.recv().unwrap() {
                PeerThreadMessage::AppendEntries(entry) => {
                    assert_eq!(entry.entries.len(), vec.len());
                    assert_eq!(entry.entries, vec);
                },
                _ => panic!()
            }
        }
    }

    /// Makes sure peer threads receive requestVote messages
    /// when an election is started.
    #[test]
    fn server_starts_election() {
        const NUM_PEERS: u64 = 4;
        let (rx, _, s) = mock_server(NUM_PEERS);
        let (ref state_ref, ref cvar) = *s.state;
        let mut state = state_ref.lock().unwrap();
        s.start_election(&mut state, cvar);
        assert!(matches!(state.current_state, State::Candidate{ .. }));
        // Each peer should have received a RequestVote message.
        for _ in 0..s.info.peers.len() {
            match rx.recv().unwrap() {
                PeerThreadMessage::RequestVote(vote) => {
                    assert_eq!(vote.term, state.current_term);
                    assert_eq!(vote.candidate_id, s.info.me.0);
                },
                _ => panic!()
            }
        }
    }

    #[test]
    fn server_updates_commit_index() {
        const NUM_PEERS: u64 = 4;
        let (_, rx2, mut s) = mock_server(NUM_PEERS);
        let (ref state_ref, ref cvar) = *s.state;
        let mut state = state_ref.lock().unwrap();
        { // Change state
            s.info.peers[0].match_index = 2;
            s.info.peers[1].match_index = 1;
            s.info.peers[2].match_index = 2;
            s.info.peers[3].match_index = 3;
        }
        update_commit_index(&s.info, &mut state, &cvar);
        assert_eq!(state.commit_index, 2);
        // make sure state machine got a command
        match rx2.recv().unwrap() {
            StateMachineMessage::Command(_) => (),
            _ => panic!(),
        }
    }

    #[test]
    fn main_thread_increments_num_votes_when_candidate() {
        let (_rx, _, mut s) = mock_server(4);
        // trigger an election
        {
            let mut state = s.state.0.lock().unwrap();
            s.start_election(&mut state, &s.state.1);
            assert!(matches!(state.current_state, State::Candidate{ num_votes: 1, .. }));
        }

        let request_vote_reply = RequestVoteReply {
            term: 1,
            vote_granted: true
        };

        let mut state = s.state.0.lock().unwrap();
        handle_request_vote_reply(request_vote_reply, &mut s.info, &mut state, &s.state.1, s.log);

        assert!(matches!(state.current_state, State::Candidate{ num_votes: 2, .. }));
    }

    // Mocks casting a vote for this server
    fn cast_vote (s: &mut Server) {
        let request_vote_reply = RequestVoteReply {
            term: 1,
            vote_granted: true
        };
        let mut state = s.state.0.lock().unwrap();
        // we've already voted for ourselves
        handle_request_vote_reply(request_vote_reply.clone(), &mut s.info, &mut state, &s.state.1, s.log.clone());
    }

    // Creates a mock sever with |num_peers| peers and send 1 too few votes to win
    // the election
    fn main_thread_simulate_half_votes(num_peers: u64) -> (Receiver<PeerThreadMessage>, Server) {
        let (rx, _, mut s) = mock_server(num_peers);
        // trigger an election
        {
            let mut state = s.state.0.lock().unwrap();
            s.start_election(&mut state, &s.state.1);
            assert!(matches!(state.current_state, State::Candidate{ num_votes: 1, .. }));
        }

        // there are peers.size() + 1 (ourself) servers in the cluster.
        // You need exactly half plus 1 votes to win the election
        let num_votes_required = (s.info.peers.len() + 1) / 2 + 1;

        let num_votes_testing = num_votes_required - 1;

        // we've already voted for ourselves
        for _ in 0 .. num_votes_testing - 1 {
            cast_vote (&mut s);
        }

        (rx, s)
    }

    #[test]
    // Check that we are still a candidate after recieving one too few votes to win
    // with an even number of peers
    fn does_not_become_leader_early_even_num_peers() {
        const NUM_PEERS: u64 = 20;
        let (_, s) = main_thread_simulate_half_votes(NUM_PEERS);

        let state = s.state.0.lock().unwrap();
        let _correct_vote_count = NUM_PEERS + 1;
        assert!(matches!(state.current_state, State::Candidate{ num_votes: _correct_vote_count, .. }));
    }

    #[test]
    // Check that we are still a candidate after recieving one too few votes to win
    // with an odd number of peers
    fn does_not_become_leader_early_odd_num_peers() {
        const NUM_PEERS: u64 = 51;
        let (_, s) = main_thread_simulate_half_votes(NUM_PEERS);

        let state = s.state.0.lock().unwrap();
        let _correct_vote_count = NUM_PEERS + 1;
        assert!(matches!(state.current_state, State::Candidate{ num_votes: _correct_vote_count, .. }));
    }

    // Returns a server that has mocked out being elected leader
    fn mock_leader_server(num_peers: u64) -> (Receiver<PeerThreadMessage>, Server) {
        let (rx, mut s) = main_thread_simulate_half_votes(num_peers);

        // cast one more vote
        cast_vote (&mut s);

        (rx, s)
    }

    #[test]
    fn becomes_leader_after_election_even_num_peers() {
        const NUM_PEERS: u64 = 50;
        let (_rx, mut s) = main_thread_simulate_half_votes(NUM_PEERS);

        // cast one more vote
        cast_vote (&mut s);
        let state = s.state.0.lock().unwrap();
        assert!(matches!(state.current_state, State::Leader{ .. }));
    }

    #[test]
    fn becomes_leader_after_election_odd_num_peers() {
        const NUM_PEERS: u64 = 51;
        let (_rx, s) = mock_leader_server(NUM_PEERS);

        let state = s.state.0.lock().unwrap();
        assert!(matches!(state.current_state, State::Leader{ .. }));
    }

    #[test]
    fn throws_away_votes_cast_after_elected() {
        const NUM_PEERS: u64 = 86;

        let (_rx, mut s) = mock_leader_server(NUM_PEERS);

        let prev_state: ServerState = {
            let state: &ServerState = &(s.state.0.lock().unwrap());
            state.clone()
        };
        // cast one more vote
        cast_vote(&mut s);
        let state: &ServerState = &s.state.0.lock().unwrap();
        assert_eq!(*state, prev_state);
    }

    #[test]
    fn throws_away_votes_after_losing_election() {
        const NUM_PEERS: u64 = 85;
        // almost win an election
        let (_rx, mut s) = main_thread_simulate_half_votes(NUM_PEERS);
        // lose the election :(
        let prev_state: ServerState = {
            let state: &mut ServerState = &mut s.state.0.lock().unwrap();
            state.transition_to_follower(1, &s.state.1);
            assert!(matches!(state.current_state, State::Follower));
            state.clone()
        };
        // cast one more vote
        cast_vote(&mut s);
        let state: &ServerState = &s.state.0.lock().unwrap();
        assert_eq!(*state, prev_state);
    }

    // TODO: Come up with a consistent way to structure modules and unit tests 
    mod server_state {
        use super::mock_server;
        use super::super::{State};
        use super::super::peer::{PeerThreadMessage};
        use super::super::log::{Op};

        // TODO (sydli) make sure cv is called
        #[test]
        fn transition_to_candidate_normal() {
            const OUR_ID: u64 = 5;
            const NUM_PEERS: u64 = 4;
            let (_, _, s) = mock_server(NUM_PEERS);
            let (ref state_ref, ref cvar) = *s.state;
            let mut state = state_ref.lock().unwrap();
            assert_eq!(state.current_term, 0);
            assert_eq!(state.voted_for, None);
            assert!(matches!(state.current_state, State::Follower));

            state.transition_to_candidate(OUR_ID, cvar);
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
            const NUM_PEERS: u64 = 4;
            let (rx, _, mut s) = mock_server(NUM_PEERS);
            let (ref state_ref, ref cvar) = *s.state;
            let mut state = state_ref.lock().unwrap();
            // we must be a candidate before we can become a leader
            state.transition_to_candidate(OUR_ID, cvar);
            state.transition_to_leader(&mut s.info, s.log.clone(), cvar);
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
                        // Since we march forwards all the peer next_index to
                        // our commit index when the leader changes, the first broadcast
                        // should actually just be a heartbeat.
                        assert_eq!(msg.entries.len(), 1);
                        assert_eq!(*entry, msg.entries[0]);
                    },
                    _=> panic!("Incorrect message type sent in response to transition to leader")
                }
            }
        }

        #[test]
        fn transition_to_follower_normal() {
            const OUR_ID: u64 = 1;
            const NUM_PEERS: u64 = 4;
            let (_, _, s) = mock_server(NUM_PEERS);
            let (ref state_ref, ref cvar) = *s.state;
            let mut state = state_ref.lock().unwrap();
            state.transition_to_candidate(OUR_ID, cvar);
            assert_eq!(state.current_term, 1);
            assert_eq!(state.voted_for, Some(OUR_ID));
            state.transition_to_follower(1, cvar);
            assert_eq!(state.current_term, 1);
            assert_eq!(state.voted_for, Some(OUR_ID));
            assert!(matches!(state.current_state, State::Follower));
        }

        #[test]
        fn transition_to_follower_wipes_vote_if_new_term() {
            const OUR_ID: u64 = 1;
            const NEW_TERM: u64 = 2;
            const NUM_PEERS: u64 = 4;
            let (_, _, s) = mock_server(NUM_PEERS);
            let (ref state_ref, ref cvar) = *s.state;
            let mut state = state_ref.lock().unwrap();
            state.transition_to_candidate(OUR_ID, cvar);
            assert_eq!(state.current_term, 1);
            assert_eq!(state.voted_for, Some(OUR_ID));
            state.transition_to_follower(NEW_TERM, cvar);
            assert_eq!(state.current_term, NEW_TERM);
            assert_eq!(state.voted_for, None);
            assert!(matches!(state.current_state, State::Follower));
        }
    }
}
