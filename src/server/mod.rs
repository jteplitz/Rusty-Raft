mod log;
mod peer;
mod state_machine;
mod state_file;
use capnp;
use rand;
use raft_capnp::{append_entries, append_entries_reply,
                 request_vote, request_vote_reply,
                 client_request, entry};
use capnp::message;
use capnp::serialize_packed;
use rpc::{RpcError};
use rpc::server::{RpcObject, RpcServer};
use client::state_machine::{RaftStateMachine, StateMachine};
use common::{Config, RaftError,
             raft_command,
             raft_query,
             client_command};
use common::constants;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use std::thread;
use std::thread::JoinHandle;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender, Receiver, RecvTimeoutError};
use std::mem;
use std::cmp::min;
use std::collections::{HashMap, VecDeque};
use std::io::{Error as IoError, BufWriter};
use std::fs::{File, OpenOptions};
use rand::distributions::{IndependentSample, Range};
use rand::{thread_rng, Rng};
use self::log::{Log, Entry};
use self::state_machine::{StateMachineMessage, state_machine_thread, StateMachineHandle};
use self::peer::{Peer, PeerHandle, PeerThreadMessage, RequestVoteMessage, PeerState, NonVotingPeerState, PeerInfo};
use self::state_file::StateFile;

pub type RpcHandlerPipe = Sender<Result<(), RaftError>>;

/// Messages that can be sent to the main thread.
/// *Reply messages encapsulate replies from peer machines, and
/// ClientAppendRequest is a message sent by the client thread.
pub struct AppendEntriesReply {
    term: u64,
    commit_index: usize,
    peer: PeerInfo,
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
    ClientAppendRequest (raft_command::Request),
    Shutdown,
    EntryPersisted (usize),
    AddServer(PeerInfo, RpcHandlerPipe),
    RemoveServer(PeerInfo, RpcHandlerPipe)
}

// TODO: RW locks?
// NB: State lock should never be acquired while holding the log lock
struct Server {
    // TODO: Rename properties?
    state: Arc<Mutex<ServerState>>,
    log: Arc<Mutex<Log>>,
    info: ServerInfo
}

struct ServerInfo {
    state_machine: StateMachineHandle,
    me: (u64, SocketAddr),
    heartbeat_timeout: Duration,
    to_me: Sender<MainThreadMessage>
}

// States that each machine can be in!
#[derive(Debug, Clone)]
enum State {
    Candidate { num_votes: usize, start_time: Instant },
    Leader { 
        last_heartbeat: Instant,
        pending_cluster_changes: VecDeque<(PeerInfo, RpcHandlerPipe)>,
        uncommited_cluster_change: Option<(usize, RpcHandlerPipe)>
    },
    Follower,
}

/// Store's the state that the server is currently in along with the current_term
/// and current_id. These fields should all share a lock.
#[derive(Debug)]
pub struct ServerState {
    current_state: State,
    current_term: u64,
    commit_index: usize,
    last_persisted_index: usize,
    last_leader_contact: (Instant, Option<u64>),
    voted_for: Option<u64>,
    election_timeout: Duration,
    state_file: StateFile,
    peers: HashMap<u64, PeerHandle>
}

/// 
/// Implements state transitions
///
impl ServerState {
    ///
    /// Transitions into the candidate state by incrementing the current_term and resetting the
    /// current_index.
    /// This should only be called if we're in the follower state or already in the candidate state
    /// NB: If we're the only server in the cluster (according to the latset config in our log)
    /// then we transition directly to leader
    ///
    fn transition_to_candidate(&mut self, info: &mut ServerInfo, log: Arc<Mutex<Log>>) -> Result<(), IoError> {
        debug_assert!(matches!(self.current_state, State::Follower) ||
                      matches!(self.current_state, State::Candidate { .. }));
        self.current_state = State::Candidate { start_time: Instant::now(), num_votes: 1 }; // we always start with 1 vote from ourselves
        self.current_term += 1;
        self.voted_for = Some(info.me.0); // vote for ourselves
        self.election_timeout = generate_election_timeout();
        self.state_file.save_state(state_file::State {term: self.current_term, voted_for: self.voted_for})?;

        // start up peer threads
        let cluster_config = log.lock().unwrap().get_cluster_config();
        match cluster_config {
            Some(config) => {
                self.peers = config.into_iter()
                .filter(|&(id, addr)| id != info.me.0)
                .map(|(id, addr)| {
                    (id, Peer::start((id, addr), info.to_me.clone(), None))
                })
                .collect();
                if self.peers.len() == 0 {
                    // We win an election if we're the only server in the cluster and have
                    // a valid config
                    self.transition_to_leader(info, log);
                }
            },
            None => {/*No config. There's hope that if we wait we'll hear from a leader about a cluster config*/}
        };

        Ok(())
    }

    ///
    /// Transitions into the leader state from the candidate state.
    ///
    fn transition_to_leader(&mut self, info: &mut ServerInfo, log: Arc<Mutex<Log>>) {
        debug_assert!(matches!(self.current_state, State::Candidate{ .. }));
        // Append dummy entry.
        let noop_index = { // Scope the log lock so we drop it immediately afterwards.
            log.lock().unwrap().append_entry(Entry::noop(self.current_term))
                .get_last_entry_index()
        };
        self.current_state = State::Leader { 
            last_heartbeat: Instant::now(),
            pending_cluster_changes: VecDeque::new(),
            uncommited_cluster_change: None
        };
        for (_, peer) in &mut self.peers {
            peer.next_index = self.commit_index + 1;
        }
        self.commit_index = self.commit_index + 1;
        broadcast_append_entries(info, self, log.clone());

        trace!("Server {}: Became leader for term {}", info.me.0, self.current_term)
    }

    ///
    /// Transitions into the follower state from either the candidate state or the leader state
    /// If the new term is greater than our current term this also resets
    /// our vote tracker.
    ///
    fn transition_to_follower(&mut self, new_term: u64,
                              to_state_machine: &Sender<StateMachineMessage>,
                              voted_for: Option<u64>, log: Arc<Mutex<Log>>) -> Result<(), IoError> {
        debug_assert!(new_term >= self.current_term);
        if let State::Leader {ref pending_cluster_changes, ref uncommited_cluster_change, ..} = self.current_state {
            to_state_machine.send(StateMachineMessage::Flush).unwrap();
            log.lock().unwrap().flush_background_thread();

            // Send NOT_LEADER errors to any clients waiting on pending cluster changes
            // they can retry with the new leader
            let pipes = pending_cluster_changes
                .iter()
                .map(|&(_, ref pipe)| {
                    pipe
                })
                .chain(pending_cluster_changes.iter().map(|&(_, ref pipe)| pipe));
            for pipe in pipes {
                warn!("Sending not leader!");
                // TODO: Send leader guess
                pipe.send(Err(RaftError::NotLeader(None)));
            }
        }
        // Drop peers
        self.peers.clear();

        self.voted_for = voted_for;
        self.current_term = new_term;
        self.current_state = State::Follower;
        self.election_timeout = generate_election_timeout();
        
        self.state_file.save_state(state_file::State {term: self.current_term, voted_for: self.voted_for})?;
        Ok(())
    }
}

///
/// Updates the server's commit index to the median of our peers' indices.
///
/// #Panics
/// Panics if called while not leader
///
fn update_commit_index(server_info: &mut ServerInfo, state: &mut ServerState, log: Arc<Mutex<Log>>) {
    // Find median of all (voting) peer commit indices.
    let mut indices: Vec<usize> = state.peers.iter()
        .filter(|&(_,  peer)| matches!(peer.state, PeerState::Voting))
        .map(|(_, peer)| peer.match_index).collect();
    indices.sort();
    let new_index;
    if indices.len() == 0 {
        // if there are no other servers in the cluster
        // then our commit index should be in lockstep with our
        // persisted entries
        new_index = state.last_persisted_index;
    } else {
        new_index = *indices.get( indices.len() / 2 ).unwrap();
    }
    // Set new commit index if it's higher and it has been persisted to disk!
    if new_index <= state.commit_index || new_index > state.last_persisted_index { return; }
    state.commit_index = new_index;
    server_info.state_machine.tx.send(
        StateMachineMessage::Commit(state.commit_index)).unwrap();

    let mut next_cluster_change;
    match state.current_state {
        State::Leader{ref mut pending_cluster_changes, ref mut uncommited_cluster_change, ..} => {
            next_cluster_change = match *uncommited_cluster_change {
                None => pending_cluster_changes.pop_front(),
                Some((index, ref pipe)) => {
                    if new_index >= index {
                        // cluster change commited
                        // notify rpc handler
                        pipe.send(Ok(())).unwrap();
                        pending_cluster_changes.pop_front()
                    } else {
                        None
                    }
                }
            }
        },
        State::Follower | State::Candidate{..} => {
            panic!("Call to update_commit_index while not leader.");
        }
    }

    match next_cluster_change {
        Some((peer, pipe)) => add_server_to_log(peer, pipe, server_info, state, log),
        None => {
            // we're not waiting on a cluster change
            if let State::Leader{ref mut uncommited_cluster_change, ..} = state.current_state {
                // always executes because the state is locked and we made it this far
                *uncommited_cluster_change = None;
            }
        }
    };
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
                    mem::replace(&mut self.rpc_server, None);
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

/// Starts a new test server 
/// If an address is provided it should be the address of this server
/// This will get written to the log before the server starts up,
/// so it should only be given to the first server in the cluster
///
/// NB: The server won't actually be listening on relay_addr. It starts up on a random port,
/// so it can be used in parallel tests (you should use the relay server to send messages to
/// this server)
/// 
/// To start a real server use `start_server`
pub fn start_test_server<F> (id: u64, state_machine: F, 
                         state_filename: &str, log_filename: &str,
                         relay_addr: Option<SocketAddr>) -> Result<ServerHandle, IoError> 
    where F: FnOnce() -> Box<RaftStateMachine> {
    const HEARTBEAT_TIMEOUT: u64 = 75;

    if let Some(addr) = relay_addr {
        write_initial_config_to_log(&log_filename, id, relay_addr.unwrap())?;
    }

    let config = Config::new (id,
        "127.0.0.1:0",
        Duration::from_millis(HEARTBEAT_TIMEOUT),
        &state_filename,
        &log_filename
    );
    start_server_with_config(config, state_machine)
}

/// Starts a new server running the raft consensus algorithim.
///
/// TODO(jason): Take in filenames instead of using random ones, and only
pub fn start_server(id: u64, state_machine: Box<StateMachine>, my_addr: SocketAddr, first: bool, state_filename: String, log_filename: String) -> Result<ServerHandle, IoError> {
    const HEARTBEAT_TIMEOUT: u64 = 75;
    const STATE_FILENAME_LEN: usize = 20;

    if first { write_initial_config_to_log(&log_filename, id, my_addr)?; }

    let state_machine = RaftStateMachine::new(state_machine);
    let config = Config::new (id,
        my_addr,
        Duration::from_millis(HEARTBEAT_TIMEOUT),
        &state_filename,
        &log_filename
    );
    start_server_with_config(config, move || Box::new(state_machine))
}

fn write_initial_config_to_log(log_filename: &str, id: u64, addr: SocketAddr) -> Result<(), IoError> {
    // write the initial config to the log file
    let entry = Entry {
        index: 1,
        term: 1, 
        op: raft_command::Request::SetConfig(vec![(id, addr)])
    };
    let mut file = OpenOptions::new().write(true).read(false)
                  .create(true).truncate(true).open(log_filename)?;

    let mut writer = BufWriter::new(&mut file);
    let mut builder = message::Builder::new_default();
    entry.into_proto(&mut builder.init_root::<entry::Builder>());
    serialize_packed::write_message(&mut writer, &builder)
}

///
/// Starts up a new raft server with the given config.
/// This is mostly just a bootstrapper for now. It probably won't end up in the public API
///
pub fn start_server_with_config<F> (config: Config, load_state_machine: F) -> Result<ServerHandle, IoError>
    where F: FnOnce() -> Box<RaftStateMachine> {
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
fn handle_append_entries_reply(m: AppendEntriesReply, server_info: &mut ServerInfo,
                               state: &mut ServerState, log: Arc<Mutex<Log>>) {
    match state.current_state {
        State::Leader{ .. } => (),
        State::Candidate{ .. } | State::Follower => return, // drop it like it's hot
    };
        
    if m.term > state.current_term {
        trace!("Server {}: becoming follower in term {} after being leader in {}", server_info.me.0, m.term, state.current_term);
        match state.transition_to_follower(m.term, &server_info.state_machine.tx, None, log.clone()) {
            Ok(_) => {},
            Err(e) => error!("Unable to write to state file. We didn't vote 
                              in this election so this isn't a fatal error. However,
                              it will likely be one on the next election if the
                              issue isn't fixed: {}", e)
        };
    } else if m.success {
        // TODO: Handle non voting members here
        // On success, advance peer's index.
        state.peers.get_mut(&m.peer.0).map(|peer| {
            debug_assert!(m.commit_index >= peer.next_index - 1);
            peer.next_index = m.commit_index + 1;
            peer.match_index = m.commit_index;
            peer.advance_non_voting_peer_round(log.lock().unwrap().get_last_entry_index())
        })
        .map(|peer_state| {
            match peer_state {
                NonVotingPeerState::TimedOut(pipe) => timeout_add_server(m.peer.0, pipe, state),
                NonVotingPeerState::CaughtUp(pipe) => add_caught_up_server(m.peer, pipe, server_info, state, log),
                NonVotingPeerState::CatchingUp => {/* need to wait for another round */},
                NonVotingPeerState::VotingPeer => update_commit_index(server_info, state, log)
            }
        });
    } else {
        let leader_id = server_info.me.0;
        // If we failed, roll back peer index by 1 (if we can) and retry
        // the append entries call.
        let (commit_index, current_term) = (state.commit_index, state.current_term);
        state.peers.get_mut(&m.peer.0).map(|peer| {
            if peer.next_index > 1 {
                // can we make sure match_index never exceeds peer_index?
                peer.next_index = peer.next_index - 1;
            }
            peer.append_entries_nonblocking(leader_id,
                                            commit_index,
                                            current_term, log);
        });
    }
}

/// Either adds a caught up server directly to the cluster
/// or queues it to be added once all pending config changes
/// have commited
/// Noop if we're not leader
///
/// #Panics
/// Panics if the log lock is poisoned
fn add_caught_up_server(peer: PeerInfo, pipe: RpcHandlerPipe, info: &mut ServerInfo,
                        state: &mut ServerState, log_lock: Arc<Mutex<Log>>) {
    let log_lock_clone = log_lock.clone();
    match state.current_state {
        State::Leader{ref mut pending_cluster_changes, ref uncommited_cluster_change, ..} => {
            if uncommited_cluster_change.is_some() || log_lock.lock().unwrap().get_last_entry_term() != state.current_term {
                // It's not safe to introduce a config right now. Queue it for later
                pending_cluster_changes.push_back((peer, pipe));
                None
            } else {
                Some((peer, pipe))
            }
        },
        _ => panic!("Attempt to add a server when we aren't leader. This should be impossible.")
    }
    .map(|(peer, pipe)| add_server_to_log(peer, pipe, info, state, log_lock));
}

/// Adds the caught up server to the cluster
/// And updates our current state to indicate
/// that we are waiting on this cluster change
///
/// #Panics
/// Panics if we are not leader
fn add_server_to_log (peer: PeerInfo, pipe: RpcHandlerPipe, info: &mut ServerInfo,
                      state: &mut ServerState, log_lock: Arc<Mutex<Log>>) {
    let mut config = log_lock.lock().unwrap().get_cluster_config().unwrap();
    config.push(peer);
    let op = raft_command::Request::SetConfig(config);
    let index = append_to_log(log_lock.clone(), op, state.current_term, &state.current_state).unwrap();
    if let State::Leader{ref mut uncommited_cluster_change, ..} = state.current_state {
        // always executes because the state is locked and we made it this far
        *uncommited_cluster_change = Some((index, pipe));
    }
    broadcast_append_entries(info, state, log_lock);
}

/// Rejects this add server rpc with a timeout
/// and removes this peer from the peers map
fn timeout_add_server(peer: u64, pipe: RpcHandlerPipe, state: &mut ServerState) {
    pipe.send(Err(RaftError::Timeout)).unwrap();
    state.peers.remove(&peer);
}

/// Helper function to append a command to the log!
/// Returns this entires log index (which is not yet commited)
fn append_to_log(log: Arc<Mutex<Log>>, op: raft_command::Request, current_term: u64, current_state: &State) -> Option<usize> {
    match *current_state {
        State::Leader {.. } => {
            let entry = Entry {
                index: 0,
                term: current_term,
                op: op
            };
            Some(log.lock().unwrap().append_entry(entry).get_last_entry_index())
        },
        State::Follower | State::Candidate{..} => {
            None
            /*This is an old message. Drop it*/
        }
    }
}

///
/// Write |entry| to the log and try to replicate it across all servers.
/// This function is non-blocking; it simply forwards AppendEntries messages
/// to all known peers.
///
fn broadcast_append_entries(info: &mut ServerInfo, state: &mut ServerState, log: Arc<Mutex<Log>>) {
    // only broadcast append entries if we're leader
    if let State::Leader {ref mut last_heartbeat, ..} = state.current_state {
        for (_, peer) in &state.peers {
            // Perf: each call copies the needed |entries| from |log| to send along to the peer.
            peer.append_entries_nonblocking(info.me.0, state.commit_index,
                                            state.current_term, log.clone());
        }
        *last_heartbeat = Instant::now();
    }
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
        let num_peers = state.peers.iter().filter(|&(_, p)| matches!(p.state, PeerState::Voting)).count();
        if num_votes > (num_peers + 1) / 2{
            // Woo! We won the election
            state.transition_to_leader(info, log);
        }
    }
}

impl Server {
    fn new (config: Config, tx: Sender<MainThreadMessage>, state_machine: Box<RaftStateMachine>)
        -> Result<(Server, RpcServer), IoError> {
        let me = config.me;
        let mut state_file = StateFile::new_from_filename(&config.state_filename)?;
        let persisted_state = state_file.get_state()?;
        let (last_persisted_index, log) = {
            let l = Log::new_from_filename(config.log_filename, tx.clone())?;
            (l.get_last_entry_index(), Arc::new(Mutex::new(l)))
        };
        let state = Arc::new(Mutex::new(ServerState {
            current_state: State::Follower,
            current_term: persisted_state.term,
            commit_index: 0,
            voted_for: persisted_state.voted_for,
            last_leader_contact: (Instant::now(), None),
            election_timeout: generate_election_timeout(),
            state_file: state_file,
            peers: HashMap::new(),
            last_persisted_index: last_persisted_index
        }));

        // 1a. Start state machine thread.
        let state_machine_handle = state_machine_thread(
            log.clone(), 0, state_machine, state.clone(), tx.clone());
        let to_state_machine_locked = 
            Arc::new(Mutex::new(state_machine_handle.tx.clone()));

        // 1. Start RPC request handlers
        let append_entries_handler: Box<RpcObject> = Box::new(
            AppendEntriesHandler {state: state.clone(), log: log.clone(),
                                  to_state_machine: to_state_machine_locked.clone() }
        );
        let request_vote_handler: Box<RpcObject> = Box::new(
            RequestVoteHandler {state: state.clone(), log: log.clone(),
                                to_state_machine: to_state_machine_locked.clone() }
        );
        let client_request_handler: Box<RpcObject> = Box::new(
            ClientRequestHandler {state: state.clone(),
                                  to_state_machine: to_state_machine_locked.clone(),
                                  to_main_thread: Arc::new(Mutex::new(tx.clone()))}
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

        // 3. Construct server state object.
        // safe to unwrap here because we should only get this far
        // if the server is bound.
        let bound_address = rpc_server.get_local_addr().unwrap();
        let info = ServerInfo {
            me: (me.0, bound_address),
            heartbeat_timeout: config.heartbeat_timeout,
            state_machine: state_machine_handle,
            to_me: tx.clone()
        };

        Ok((Server {
            state: state,
            log: log,
            info: info
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
                    let state = self.state.lock().unwrap();

                    match state.current_state {
                        State::Follower => {
                            let now = Instant::now();
                            current_timeout = state.election_timeout.checked_sub(
                                now.duration_since(state.last_leader_contact.0));
                        }
                        State::Candidate{start_time, ..} => {
                            let time_since_election = Instant::now() - start_time;

                            current_timeout = state.election_timeout.checked_sub(time_since_election);
                        },
                        State::Leader{last_heartbeat, ..} => {
                            // TODO: Should last_heartbeat be in the leader state?
                            let heartbeat_timeout = self.info.heartbeat_timeout;
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

                // Handles incoming messages
                // NB: We can not assume the server is any particular state here
                // so any method called in this match will need to check the current_state
                // if its actions are state dependent
                match message {
                    Ok(m) => {
                        let ref mut state = self.state.lock().unwrap();
                        match m {
                            MainThreadMessage::AppendEntriesReply(m) => {
                                handle_append_entries_reply(m, &mut self.info, state, self.log.clone());
                            },
                            MainThreadMessage::ClientAppendRequest(command) => {
                                append_to_log(self.log.clone(), command, state.current_term, &state.current_state);
                                broadcast_append_entries(&mut self.info, state, self.log.clone());
                            },
                            MainThreadMessage::RequestVoteReply(m) =>  {
                                handle_request_vote_reply(m, &mut self.info, state, self.log.clone());
                            },
                            MainThreadMessage::EntryPersisted(index) => {
                                Server::handle_entry_persisted(index, &mut self.info, state, self.log.clone());
                            },
                            MainThreadMessage::AddServer(server_info, to_background) => {
                                // start up a peer for this server
                                // and begin catching them up
                                // Once caught up add to pending config changes queue
                                // Once it is at the front of that queue attempt to commit this
                                // server to a config entry in the log
                                // Once commited return success
                                Server::add_peer(server_info, to_background, &mut self.info, state, self.log.clone())
                            },
                            MainThreadMessage::RemoveServer(index, addr) => {
                                unimplemented!();
                            },
                            MainThreadMessage::Shutdown => {
                                break;
                            },
                        };
                    },
                    Err(_) => {
                        self.handle_timeout();
                        continue;
                    }
                }
                
            } // end loop

            // clean up based on state
            let mut state = self.state.lock().unwrap();
            match state.current_state {
                State::Leader {ref pending_cluster_changes, ..} => {
                    self.info.state_machine.tx.send(StateMachineMessage::Flush).unwrap();
                    self.log.lock().unwrap().flush_background_thread();
                    let pipes = pending_cluster_changes
                        .iter()
                        .map(|&(_, ref pipe)| {
                            pipe
                        })
                        .chain(pending_cluster_changes.iter().map(|&(_, ref pipe)| pipe));
                    for pipe in pipes {
                        warn!("Sending not leader!");
                        // TODO: Send leader guess
                        pipe.send(Err(RaftError::NotLeader(None)));
                    }
                },
                State::Follower | State::Candidate {..} => {/*No cleanup work*/}
            }
            // shutdown the peers by dropping their handles
            state.peers.clear();
        })
    }

    /// Starts up a new peer in the NonVoting state for the given address and instructs it to begin
    /// catching up
    fn add_peer(server_info: PeerInfo, to_background: RpcHandlerPipe, info: &mut ServerInfo, state: &mut ServerState,
                log: Arc<Mutex<Log>>) {
        let peer = Peer::start(server_info, info.to_me.clone(), Some(to_background));
        peer.append_entries_nonblocking(info.me.0, state.commit_index, state.current_term, log);
        state.peers.insert(peer.id, peer);
    }

    /// Updates commit index if appropiate after an entry has been commited to disk
    fn handle_entry_persisted(index: usize, info: &mut ServerInfo, state: &mut ServerState, log: Arc<Mutex<Log>>) {
        state.last_persisted_index = index;
        // commit index may need to be marched forward if it was waiting on a background disk write
        // but only if we are still leader
        match state.current_state {
            State::Leader{..} => update_commit_index(info, state, log),
            State::Candidate{..} | State::Follower => {}
        }
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
        let ref mut state = self.state.lock().unwrap();
        match state.current_state {
            State::Follower | State::Candidate{ .. } => {
                let now = Instant::now();
                if now.duration_since(state.last_leader_contact.0)
                    > state.election_timeout {
                    Server::start_election(&mut self.info, state, self.log.clone());
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
    fn start_election(info: &mut ServerInfo, state: &mut ServerState, log: Arc<Mutex<Log>>) -> Duration {
        trace!("Server {}: Starting election for term {}. Previously voted for {:?}", info.me.0,
                 state.current_term + 1, state.voted_for);
        // transition to the candidate state
        match state.transition_to_candidate(info, log.clone()) {
            Ok(_) => {},
            Err(e) => {
                warn!("Unable to write to state file: {}", e);
                // couldn't persist vote for ourself, so it is not safe to ask any peers
                // for their vote
                return state.election_timeout;
            }
        }

        let (last_log_index, last_log_term) = {
            let log = log.lock().unwrap();
            (log.get_last_entry_index(), log.get_last_entry_term())
        };

        // TODO: For consistency with append entries this should probably be done in the peerhandle 
        // TODO: If there are no peers, we are leader!

        // tell each peer to send out RequestToVote RPCs
        let request_vote_message = RequestVoteMessage {
            term: state.current_term,
            candidate_id: info.me.0,
            last_log_index: last_log_index,
            last_log_term: last_log_term
        };

        // only request votes from voting members
        for (_, peer) in state.peers.iter().filter(|&(_, peer)| matches!(peer.state, PeerState::Voting)) {
            peer.to_peer.send(PeerThreadMessage::RequestVote(request_vote_message))
                        .unwrap(); // panic if the peer thread is down
        }
        state.election_timeout
    }
}

struct RequestVoteHandler {
    state: Arc<Mutex<ServerState>>,
    log: Arc<Mutex<Log>>,
    to_state_machine: Arc<Mutex<Sender<StateMachineMessage>>>,
}

// TODO(jason): Test
impl RpcObject for RequestVoteHandler {
    fn handle_rpc (&self, params: capnp::any_pointer::Reader, result: capnp::any_pointer::Builder) 
        ->Result<(), RpcError>
    {
        let (candidate_id, term, last_log_index, last_log_term) = try!(
            params.get_as::<request_vote::Reader>()
            .map_err(RpcError::Capnp)
            .map(|params| {(
                 params.get_candidate_id(), params.get_term(),
                 params.get_last_log_index() as usize,
                 params.get_last_log_term())
            }));
        let mut vote_granted = false;
        let current_term;
        {
            let ref mut state = self.state.lock().unwrap(); // panics if mutex is poisoned
            // We may transition to a new term, but we want to avoid doing multiple
            // state transitions since we flush the information to disk each time.
            // So we save the new_term in a local variable and transition after we know
            // if we've voted for this candidate or not
            let new_term;
            let voted_for;
            if term > state.current_term {
                new_term = term;
                voted_for = None;
            } else {
                new_term = state.current_term;
                voted_for = state.voted_for;
            }
            let now = Instant::now();
            // only grant our vote if we've noticed a timeout
            let timed_out = now.duration_since(state.last_leader_contact.0) >= 
                            Duration::from_millis(constants::ELECTION_TIMEOUT_MIN);

            if timed_out && (voted_for == None || voted_for == Some(candidate_id)) {
                let log_is_valid = {
                    let log = self.log.lock().unwrap(); // panics if mutex is poisoned
                    log.is_other_log_valid(last_log_index, last_log_term)
                };
                if term == new_term && log_is_valid {
                    // grant vote and become a follower of this candidate
                    match state.transition_to_follower(new_term, &self.to_state_machine.lock().unwrap(), Some(candidate_id), self.log.clone()) {
                        Ok(_) => {
                            vote_granted = true;
                            state.last_leader_contact = (Instant::now(), Some(candidate_id));
                        },
                        Err(e) => {
                            vote_granted = false;
                            warn!("Unable to write to state file: {}", e);
                        }
                    };
                }
            }

            if new_term > state.current_term {
                // We did not vote for this leader, but did learn of a new term
                // so potentially need to step down and we need to persist this information to disk
                match state.transition_to_follower(new_term, &self.to_state_machine.lock().unwrap(), None, self.log.clone()) {
                    Ok(_) => {},
                    Err(e) => warn!("Unable to write to state file: {}", e)
                };
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
    log: Arc<Mutex<Log>>,
    to_state_machine: Arc<Mutex<Sender<StateMachineMessage>>>,
}

impl AppendEntriesHandler {
    ///# Panics 
    /// * Panics if there are any errors with Disk IO
    /// * Panics if the main thread or the state machine thread have panicked
    fn handle_message(&self, message: append_entries::Reader, reply: &mut append_entries_reply::Builder) {
        let ref mut state = self.state.lock().unwrap();
        let current_term = state.current_term;
        reply.set_success(false);
        reply.set_term(current_term);

        // Check: If our term doesn't match the message's term...
        if message.get_term() < current_term { return; }
        if message.get_term() > current_term || 
            (message.get_term() == current_term && matches!(state.current_state, State::Candidate{..})) {
            // Become follower for the higher term
            state.transition_to_follower(
                message.get_term(), &self.to_state_machine.lock().unwrap(), None, self.log.clone()).unwrap();
            reply.set_term(current_term);
        }

        debug_assert!(message.get_term() == state.current_term);
        // Reset election timer for this term.
        state.last_leader_contact = (Instant::now(), Some(message.get_leader_id()));
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
            log.roll_back(prev_log_index).unwrap();
            log.append_entries_blocking(entries).unwrap();
            min(log.get_last_entry_index(), message.get_leader_commit() as usize)
        };
        debug_assert!(matches!(state.current_state, State::Follower));
        // March forward the commit index.
        if commit_index > state.commit_index {
            state.commit_index = commit_index;
            self.to_state_machine.lock().unwrap()
                .send(StateMachineMessage::Commit(state.commit_index)).unwrap();
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

struct ClientRequestHandler {
    state: Arc<Mutex<ServerState>>,
    to_state_machine: Arc<Mutex<Sender<StateMachineMessage>>>,
    to_main_thread: Arc<Mutex<Sender<MainThreadMessage>>>
}

impl ClientRequestHandler {
    fn client_write_blocking(&self, op: raft_command::Request)
        -> Result<raft_command::Reply, RaftError>
    {
        let (to_me, from_sm) = channel();
        self.to_state_machine.lock().unwrap().send(
            StateMachineMessage::Command {
                command: op,
                response_channel: to_me,
            }).unwrap();
        from_sm.recv().unwrap()
    }

    ///
    /// Client read. Blocks until most recent write is properly committed to the log,
    /// then returns result from client state machine query.
    ///
    fn client_read_blocking(&self, op: raft_query::Request)
        -> Result<raft_query::Reply, RaftError>
    {
        let (to_me, from_sm) = channel();
        self.to_state_machine.lock().unwrap().send(
            StateMachineMessage::Query {
                query: op,
                response_channel: to_me,
            }).unwrap();
        from_sm.recv().unwrap()
    }

    fn add_server_blocking(&self, server_info: PeerInfo) -> Result<(), RaftError> {
        let (to_me, from_main) = channel();
        self.to_main_thread.lock().unwrap().send(
            MainThreadMessage::AddServer(server_info, to_me)
        ).unwrap();
        from_main.recv().unwrap()
    }
}

impl RpcObject for ClientRequestHandler {
    fn handle_rpc (&self, params: capnp::any_pointer::Reader,
                   result: capnp::any_pointer::Builder) -> Result<(), RpcError> {
        params.get_as::<client_request::Reader>().map(|client_request| {
            let (is_leader, mut reply) = {
                let state = self.state.lock().unwrap();
                (matches!(state.current_state, State::Leader { .. }),
                 Err(RaftError::NotLeader(state.last_leader_contact.1)))
            };

            if is_leader { 
                let op = client_command::request_from_proto(client_request);
                reply = match op {
                    client_command::Request::Command(op) =>
                        self.client_write_blocking(op).map(client_command::Reply::Command),
                    client_command::Request::Query(op) =>
                        self.client_read_blocking(op).map(client_command::Reply::Query),
                    client_command::Request::AddServer(server_info) => {
                        self.add_server_blocking(server_info)
                            .map(|_| client_command::Reply::AddServer)
                    }
                    client_command::Request::RemoveServer(server_info) => {
                        unimplemented!()
                    }
                };
            }
            let mut reply_proto = result.init_as::<client_request::reply::Builder>();
            client_command::reply_to_proto(reply, &mut reply_proto);
        })
        .map_err(RpcError::Capnp)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use super::*;
    use super::rand::{thread_rng, Rng};
    use super::peer::{PeerThreadMessage, PeerHandle, PeerState};
    use super::{ServerInfo, broadcast_append_entries, ServerState,
                State, generate_election_timeout, update_commit_index,
                StateMachineMessage,
                handle_request_vote_reply};
    use super::log::{Log, random_entries_with_term};
    use super::log::mocks::{new_mock_log, MockLogFileHandle};
    use std::time::{Duration, Instant};
    use std::sync::mpsc::{channel, Receiver};
    use std::sync::{Arc, Mutex};
    use std::fs;
    use std::collections::VecDeque;

    const DEFAULT_HEARTBEAT_TIMEOUT_MS: u64 = 150;

    struct MockServer {
        peer_rx: Receiver<PeerThreadMessage>,
        state_machine_rx: Receiver<StateMachineMessage>,
        server: Server,
        state_filename: String,
        _log_file: MockLogFileHandle
    }
    
    impl Drop for MockServer {
        /// Deletes state file
        fn drop (&mut self) {
            fs::remove_file(self.state_filename.clone()).unwrap();
        }
    }

    fn mock_server(num_peers: u64) -> MockServer {
        let state_filename_len = 20;
        let mut state_filename: String = thread_rng().gen_ascii_chars().take(state_filename_len).collect();
        state_filename = String::from("/tmp/") + &state_filename;

        let (mock_log, log_file_handle) = new_mock_log();
        let log: Arc<Mutex<Log>> = Arc::new(Mutex::new(mock_log));
        let (tx, rx) = channel();
        let (tx1, rx1) = channel();
        let peers = (0 .. num_peers)
            .map(|n| (n, PeerHandle {id: n, to_peer: tx.clone(),
                                 next_index: 1, match_index: 0, thread: None, state: PeerState::Voting}))
            .collect::<HashMap<u64, PeerHandle>>();
        let state = Arc::new(Mutex::new(ServerState {
            current_state: State::Follower,
            current_term: 0,
            commit_index: 0,
            voted_for: None,
            peers: peers,
            last_leader_contact: (Instant::now(), None),
            election_timeout: generate_election_timeout(),
            state_file: StateFile::new_from_filename(&state_filename).unwrap(),
            last_persisted_index: 0
        }));
        let server = Server {
            state: state,
            log: log,
            info: ServerInfo {
                me: (0, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)),
                heartbeat_timeout: Duration::from_millis(DEFAULT_HEARTBEAT_TIMEOUT_MS),
                state_machine: StateMachineHandle {tx: tx1, thread: None},
                to_me: channel().0,
            }
        };
        MockServer {peer_rx: rx, state_machine_rx: rx1, server: server,
                    state_filename: state_filename,
                    _log_file: log_file_handle}
    }

    /// Makes sure peer threads receive appendEntries msesages
    /// when the log is appended to.
    #[test]
    fn server_sends_append_entries() {
        const NUM_PEERS: u64 = 4;
        const TERM: u64 = 5;
        const NUM_ENTRIES: usize = 3;
        let mut mock_server = mock_server(NUM_PEERS);
        let vec = random_entries_with_term(NUM_ENTRIES, TERM);
        let commit_index = { // Append some random entries to log
          let mut log = mock_server.server.log.lock().unwrap();
          log.append_entries_blocking(vec.clone()).unwrap();
          log.get_last_entry_index()
        } - 1;
        // Send append entries to peers!
        let ref mut state = mock_server.server.state.lock().unwrap();
        state.commit_index = commit_index + 1;
        state.current_state = State::Leader {
            last_heartbeat: Instant::now(),
            pending_cluster_changes: VecDeque::new(),
            uncommited_cluster_change: None
        };
        broadcast_append_entries(&mut mock_server.server.info, state, mock_server.server.log.clone());

        // Each peer should receive a message...
        for _ in 0..state.peers.len() {
            match mock_server.peer_rx.recv().unwrap() {
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
        let mut mock_server = mock_server(NUM_PEERS);
        let mut state = mock_server.server.state.lock().unwrap();
        Server::start_election(&mut mock_server.server.info, &mut state, mock_server.server.log.clone());
        assert!(matches!(state.current_state, State::Candidate{ .. }));
        // Each peer should have received a RequestVote message.
        for _ in 0..state.peers.len() {
            match mock_server.peer_rx.recv().unwrap() {
                PeerThreadMessage::RequestVote(vote) => {
                    assert_eq!(vote.term, state.current_term);
                    assert_eq!(vote.candidate_id, mock_server.server.info.me.0);
                },
                _ => panic!()
            }
        }
    }

    fn mock_replicate_two_entries() -> MockServer {
        const NUM_PEERS: u64 = 4;
        let mut mock_server = mock_server(NUM_PEERS);
        {
            let mut state = mock_server.server.state.lock().unwrap();
            state.peers.get_mut(&0).unwrap().match_index = 2;
            state.peers.get_mut(&1).unwrap().match_index = 1;
            state.peers.get_mut(&2).unwrap().match_index = 2;
            state.peers.get_mut(&3).unwrap().match_index = 3;
        }

        mock_server
    }

    #[test]
    fn leader_waits_on_persisted_index() {
        let mut mock_server = mock_replicate_two_entries();
        let mut state = mock_server.server.state.lock().unwrap();
        update_commit_index(&mut mock_server.server.info, &mut state, mock_server.server.log.clone());
        assert_eq!(state.commit_index, 0);
    }

    #[test]
    fn server_updates_commit_index() {
        let mut mock_server = mock_replicate_two_entries();
        let mut state = mock_server.server.state.lock().unwrap();
        assert_eq!(state.commit_index, 0);

        state.last_persisted_index = 2;
        // we must be leader before commiting anything
        state.transition_to_candidate(&mut mock_server.server.info, mock_server.server.log.clone()).unwrap();
        state.transition_to_leader(&mut mock_server.server.info, mock_server.server.log.clone());
        update_commit_index(&mut mock_server.server.info, &mut state, mock_server.server.log.clone());
        assert_eq!(state.commit_index, 2);

        // make sure state machine got a command
        match mock_server.state_machine_rx.recv().unwrap() {
            StateMachineMessage::Commit(_) => (),
            _ => panic!(),
        }
    }

    #[test]
    fn main_thread_increments_num_votes_when_candidate() {
        const NUM_PEERS: u64 = 4;
        let mut mock_server = mock_server(NUM_PEERS);
        // trigger an election
        {
            let mut state = mock_server.server.state.lock().unwrap();
            Server::start_election(&mut mock_server.server.info, &mut state, mock_server.server.log.clone());
            assert!(matches!(state.current_state, State::Candidate{ num_votes: 1, .. }));
        }

        let request_vote_reply = RequestVoteReply {
            term: 1,
            vote_granted: true
        };

        let mut state = mock_server.server.state.lock().unwrap();
        handle_request_vote_reply(request_vote_reply, &mut mock_server.server.info, &mut state,
                                  mock_server.server.log.clone());

        assert!(matches!(state.current_state, State::Candidate{ num_votes: 2, .. }));
    }

    // Mocks casting a vote for this server
    fn cast_vote (s: &mut Server) {
        let request_vote_reply = RequestVoteReply {
            term: 1,
            vote_granted: true
        };
        let mut state = s.state.lock().unwrap();
        // we've already voted for ourselves
        handle_request_vote_reply(request_vote_reply.clone(), &mut s.info, &mut state, s.log.clone());
    }

    // Creates a mock sever with |num_peers| peers and send 1 too few votes to win
    // the election
    fn main_thread_simulate_half_votes(num_peers: u64) -> MockServer {
        let mut mock_server = mock_server(num_peers);
        {
            let s = &mut mock_server.server;
            // trigger an election
            {
                let mut state = s.state.lock().unwrap();
                Server::start_election(&mut s.info, &mut state, s.log.clone());
                assert!(matches!(state.current_state, State::Candidate{ num_votes: 1, .. }));
            }

            // there are peers.size() + 1 (ourself) servers in the cluster.
            // You need exactly half plus 1 votes to win the election
            let num_votes_required = (s.state.lock().unwrap().peers.len() + 1) / 2 + 1;

            let num_votes_testing = num_votes_required - 1;

            // we've already voted for ourselves
            for _ in 0 .. num_votes_testing - 1 {
                cast_vote (s);
            }
        }
        mock_server
    }

    #[test]
    // Check that we are still a candidate after recieving one too few votes to win
    // with an even number of peers
    fn does_not_become_leader_early_even_num_peers() {
        const NUM_PEERS: u64 = 20;
        let mut mock_server = main_thread_simulate_half_votes(NUM_PEERS);
        let s = &mut mock_server.server;

        let state = s.state.lock().unwrap();
        let _correct_vote_count = NUM_PEERS + 1;
        assert!(matches!(state.current_state, State::Candidate{ num_votes: _correct_vote_count, .. }));
    }

    #[test]
    // Check that we are still a candidate after recieving one too few votes to win
    // with an odd number of peers
    fn does_not_become_leader_early_odd_num_peers() {
        const NUM_PEERS: u64 = 51;
        let mut mock_server = main_thread_simulate_half_votes(NUM_PEERS);
        let s = &mut mock_server.server;

        let state = s.state.lock().unwrap();
        let _correct_vote_count = NUM_PEERS + 1;
        assert!(matches!(state.current_state, State::Candidate{ num_votes: _correct_vote_count, .. }));
    }

    // Returns a server that has mocked out being elected leader
    fn mock_leader_server(num_peers: u64) -> MockServer {
        let mut mock_server = main_thread_simulate_half_votes(num_peers);
        {
            let s = &mut mock_server.server;

            // cast one more vote
            cast_vote (s);
        }

        mock_server
    }

    #[test]
    fn becomes_leader_after_election_even_num_peers() {
        const NUM_PEERS: u64 = 50;
        let mut mock_server = main_thread_simulate_half_votes(NUM_PEERS);
        let s = &mut mock_server.server;

        // cast one more vote
        cast_vote (s);
        let state = s.state.lock().unwrap();
        assert!(matches!(state.current_state, State::Leader{ .. }));
    }

    #[test]
    fn becomes_leader_after_election_odd_num_peers() {
        const NUM_PEERS: u64 = 51;
        let mut mock_server = mock_leader_server(NUM_PEERS);
        let s = &mut mock_server.server;

        let state = s.state.lock().unwrap();
        assert!(matches!(state.current_state, State::Leader{ .. }));
    }

    #[test]
    fn throws_away_votes_cast_after_elected() {
        const NUM_PEERS: u64 = 86;

        let mut mock_server = mock_leader_server(NUM_PEERS);
        let s = &mut mock_server.server;

        let (prev_term, prev_commit_index) = {
            let state: &ServerState = &(s.state.lock().unwrap());
            (state.current_term, state.commit_index)
        };
        // cast one more vote
        cast_vote(s);
        let state: &ServerState = &s.state.lock().unwrap();
        assert!(matches!(state.current_state, State::Leader{ .. }));
        assert_eq!(state.current_term, prev_term);
        assert_eq!(state.commit_index, prev_commit_index);
    }

    #[test]
    fn throws_away_votes_after_losing_election() {
        const NUM_PEERS: u64 = 85;
        // almost win an election
        let mut mock_server = main_thread_simulate_half_votes(NUM_PEERS);
        let s = &mut mock_server.server;
        // lose the election :(
        let (prev_term, prev_commit_index) = {
            let state: &mut ServerState = &mut s.state.lock().unwrap();
            state.transition_to_follower(1, &s.info.state_machine.tx, Some(s.info.me.0), s.log.clone()).unwrap();
            assert!(matches!(state.current_state, State::Follower));
            (state.current_term, state.commit_index)
        };
        // cast one more vote
        cast_vote(s);
        let state: &ServerState = &s.state.lock().unwrap();
        assert!(matches!(state.current_state, State::Follower));
        assert_eq!(state.current_term, prev_term);
        assert_eq!(state.commit_index, prev_commit_index);
    }

    // TODO: Come up with a consistent way to structure modules and unit tests 
    mod server_state {
        use super::mock_server;
        use super::super::{State};
        use super::super::peer::{PeerThreadMessage};
        use super::super::super::common::{raft_command};
        use super::super::StateFile;

        #[test]
        fn transition_to_candidate_normal() {
            const NUM_PEERS: u64 = 4;
            let mut mock_server = mock_server(NUM_PEERS);
            let s = &mut mock_server.server;
            let mut state = s.state.lock().unwrap();
            assert_eq!(state.current_term, 0);
            assert_eq!(state.voted_for, None);
            assert!(matches!(state.current_state, State::Follower));

            state.transition_to_candidate(&mut s.info, s.log.clone()).unwrap();
            match state.current_state {
                State::Candidate{num_votes, ..} => {
                    assert_eq!(state.current_term, 1);
                    assert_eq!(state.voted_for, Some(s.info.me.0));
                    assert_eq!(num_votes, 1);
                },
                _ => panic!("Transition to candidate did not enter the candidate state.")
            }
        }

        #[test]
        fn transition_to_leader_normal() {
            const NUM_PEERS: u64 = 4;
            let mut mock_server = mock_server(NUM_PEERS);
            let s = &mut mock_server.server;
            let ref mut state = s.state.lock().unwrap();
            // we must be a candidate before we can become a leader
            state.transition_to_candidate(&mut s.info, s.log.clone()).unwrap();
            state.transition_to_leader(&mut s.info, s.log.clone());
            assert!(matches!(state.current_state, State::Leader{..}));

            // ensure that we appended a dummy log entry
            let log = s.log.lock().unwrap();
            assert_eq!(log.get_last_entry_index(), 1);
            let entry = log.get_entry(1).unwrap();
            assert!(matches!(entry.op, raft_command::Request::Noop));

            // ensure that the dummy entry was broadcasted properly
            for _ in 0..state.peers.len() {
                match mock_server.peer_rx.recv().unwrap() {
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
            let mut mock_server = mock_server(NUM_PEERS);
            let s = &mut mock_server.server;
            let ref mut state = s.state.lock().unwrap();

            state.transition_to_candidate(&mut s.info, s.log.clone()).unwrap();
            assert_eq!(state.current_term, 1);
            assert_eq!(state.voted_for.unwrap(), s.info.me.0);
            state.transition_to_follower(1, &s.info.state_machine.tx, Some(OUR_ID), s.log.clone()).unwrap();
            assert_eq!(state.current_term, 1);
            assert_eq!(state.voted_for, Some(OUR_ID));
            assert!(matches!(state.current_state, State::Follower));
        }

        #[test]
        fn transition_to_follower_wipes_vote_if_new_term() {
            const NEW_TERM: u64 = 2;
            const NUM_PEERS: u64 = 4;
            let mut mock_server = mock_server(NUM_PEERS);
            let s = &mut mock_server.server;
            let ref mut state = s.state.lock().unwrap();
            state.transition_to_candidate(&mut s.info, s.log.clone()).unwrap();
            assert_eq!(state.current_term, 1);
            assert_eq!(state.voted_for.unwrap(), s.info.me.0);
            state.transition_to_follower(NEW_TERM, &s.info.state_machine.tx, None, s.log.clone()).unwrap();
            assert_eq!(state.current_term, NEW_TERM);
            assert_eq!(state.voted_for, None);
            assert!(matches!(state.current_state, State::Follower));
        }

        #[test]
        fn transition_to_follower_persists_state() {
            const NUM_PEERS: u64 = 4;
            let mut mock_server = mock_server(NUM_PEERS);
            let s = &mut mock_server.server;
            let mut state = s.state.lock().unwrap();
            state.transition_to_follower(1, &s.info.state_machine.tx, None, s.log.clone()).unwrap();

            let mut state_file = StateFile::new_from_filename(&mock_server.state_filename).unwrap();
            let state = state_file.get_state().unwrap();
            assert_eq!(state.term, 1);
            assert_eq!(state.voted_for, None);
        }

        #[test]
        fn transition_to_follower_persists_vote() {
            const NUM_PEERS: u64 = 4;
            const VOTE_ID: u64 = 3;
            let mut mock_server = mock_server(NUM_PEERS);
            let s = &mut mock_server.server;
            let mut state = s.state.lock().unwrap();
            state.transition_to_follower(1, &s.info.state_machine.tx, Some(VOTE_ID), s.log.clone()).unwrap();

            let mut state_file = StateFile::new_from_filename(&mock_server.state_filename).unwrap();
            let state = state_file.get_state().unwrap();
            assert_eq!(state.term, 1);
            assert_eq!(state.voted_for, Some(VOTE_ID));
        }
        
        #[test]
        fn transition_to_candidate_persists_state() {
            const NUM_PEERS: u64 = 4;
            let mut mock_server = mock_server(NUM_PEERS);
            let s = &mut mock_server.server;

            let mut state = s.state.lock().unwrap();
            state.transition_to_candidate(&mut s.info, s.log.clone()).unwrap();

            let mut state_file = StateFile::new_from_filename(&mock_server.state_filename).unwrap();
            let state = state_file.get_state().unwrap();
            assert_eq!(state.term, 1);
            assert_eq!(state.voted_for.unwrap(), s.info.me.0);
        }
    }
}
