use std::mem;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::thread::JoinHandle;

use super::super::client::state_machine::{ExactlyOnceStateMachine};
use super::super::common::{RaftError};
use super::log::{Log, Op};

///
/// Messages to be sent to the state machine thread.
///
pub enum StateMachineMessage {
    Command (usize),
    Query { buffer: Vec<u8>, response_channel: Sender<Result<Vec<u8>, RaftError>> },
    Shutdown
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
pub fn state_machine_thread (log: Arc<Mutex<Log>>,
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

///
/// Applies entries (next_index, to_commit) exclusive, exclusive from |log|
/// to the |state_machine|. Returns the new committed index.
pub fn apply_commands(next_index: usize, to_commit: usize,
                  log: Arc<Mutex<Log>>,
                  state_machine: &Box<ExactlyOnceStateMachine>)
        -> usize {
    if to_commit < next_index { return next_index; }
    let to_apply = { log.lock().unwrap().get_entries_from(next_index - 1)
        [.. (to_commit - next_index + 1) ].to_vec() };
    for entry in to_apply.into_iter() {
        match entry.op {
            Op::Write{data, session} => {
                state_machine.command(&data, session).unwrap();
            },
            Op::OpenSession => {state_machine.new_session();},
            Op::Read(_) | Op::Noop | Op::Unknown => {},
        };
    }
    to_commit + 1
}

pub struct StateMachineHandle {
    pub thread: Option<JoinHandle<()>>,
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

