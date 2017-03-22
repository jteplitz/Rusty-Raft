use std::mem;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::thread::JoinHandle;

use super::{MainThreadMessage, ServerState};
use super::super::client::state_machine::ExactlyOnceStateMachine;
use super::super::common::{RaftError, raft_command, raft_query};
use super::log::{Log};

///
/// Messages to be sent to the state machine thread.
///
#[derive(Clone)]
pub enum StateMachineMessage {
    Command {
        command: raft_command::Request,
        response_channel: Sender<Result<raft_command::Reply, RaftError>>
    },
    Query { 
        query: raft_query::Request, 
        response_channel: Sender<Result<raft_query::Reply, RaftError>>
    },
    Commit (usize),
    Flush,
    Shutdown
}

///
/// A handle to the state machine thread.
/// Can send messages to it via |tx|.
pub struct StateMachineHandle {
    pub thread: Option<JoinHandle<()>>,  // The actual thread handle.
    pub tx: Sender<StateMachineMessage>  // Channel to send messages.
}

///
/// Starts thread responsible for performing operations on state machine.
/// Loops waiting on a channel for operations to perform.
///
/// |Commit| messages first advances an internal index pointer, then performs
/// StateMachine::write on the log entry's command buffer at that index if it
/// is a Write operation. This index is initialized at |start_index|.
///
/// |Query| messages perform StateMachine::read on the |query_buffer|, and
/// sends the result over |response_channel|.
///
/// Returns a handle to this thread.
///
pub fn state_machine_thread (log: Arc<Mutex<Log>>,
                             start_index: usize,
                             mut state_machine: Box<ExactlyOnceStateMachine>,
                             state: Arc<Mutex<ServerState>>,
                             to_main: Sender<MainThreadMessage>,
                            ) -> StateMachineHandle {
    let mut outstanding_commands = Vec::new();
    let(to_state_machine, from_main) = channel();
    let t = thread::spawn(move || {
        let mut next_index = start_index + 1;
        loop {
            let message: StateMachineMessage = from_main.recv().unwrap();
            let message_copy = message.clone();
            match message {
                StateMachineMessage::Commit(commit_index) => {
                    // Once the commit index has marched forward, we can
                    // apply any outstanding commands. This will also take
                    // care of ACK'ing client requests in |outstanding_commands|.
                    next_index = apply_commands(next_index, commit_index,
                                                log.clone(), &mut state_machine,
                                                &mut outstanding_commands);
                },
                StateMachineMessage::Query { query, response_channel } => {
                    // Since this thread "linearizes" commit index updates wrt queries,
                    // it's safe just to perform the query here and return it.
                    response_channel.send(state_machine.query(&query)).unwrap();
                },
                StateMachineMessage::Command { command, .. } => {
                    // Inform main thread of client append request.
                    to_main.send(MainThreadMessage::ClientAppendRequest(command))
                           .unwrap();
                    // Queue this command for later... until it's been
                    // committed.
                    outstanding_commands.push(message_copy);
                },
                StateMachineMessage::Flush => {
                    // Flush any outstanding client requests. This will only happen
                    // if leadership changes (we're not leader anymore) in which case
                    // we should forward them the new leader.
                    while let Some(cmd) = outstanding_commands.pop() {
                        if let StateMachineMessage::Command
                            { command, response_channel} = cmd {
                            response_channel.send(
                                Err(RaftError::NotLeader(
                                { state.lock().unwrap().last_leader_contact.1 })))
                            .unwrap();
                        }
                    }
                },
                // TODO: Allow state machines to provide custom shutdown logic?
                StateMachineMessage::Shutdown => break
            }
        }
    });
    StateMachineHandle {thread: Some(t), tx: to_state_machine}
}

///
/// Applies entries (next_index, to_commit) exclusive, exclusive from |log|
/// to the |state_machine|. Returns the new committed index.
pub fn apply_commands(next_index: usize, to_commit: usize,
                      log: Arc<Mutex<Log>>,
                      state_machine: &mut Box<ExactlyOnceStateMachine>,
                      outstanding_messages: &mut Vec<StateMachineMessage>)
        -> usize {
    if to_commit < next_index { return next_index; }
    let to_apply = { log.lock().unwrap().get_entries_from(next_index - 1)
        [.. (to_commit - next_index + 1) ].to_vec() };
    for entry in to_apply.into_iter() {
        let response = state_machine.command(&entry.op);
        let peek = { outstanding_messages.first().cloned() };
        if let Some(message) = peek {
            if let StateMachineMessage::Command
                {ref command, ref response_channel} = message {
                if *command == entry.op {
                    response_channel.send(response).unwrap();
                    outstanding_messages.remove(0);
                }
            }
        }
    }
    to_commit + 1
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

