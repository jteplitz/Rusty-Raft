use rusty_raft::server::*;
use rusty_raft::state_machine::*;
use std::sync::mpsc::{Sender};
use std::sync::Mutex;

/// Mock state machine that drops queries and sends commands down through the channel
pub struct MockStateMachine {
    // TODO: I don't think we should need to put this into a mutex
    commands: Mutex<Sender<Vec<u8>>>
}

impl MockStateMachine {
    pub fn new_with_sender(sender: Sender<Vec<u8>>) -> ExactlyOnceStateMachine {
        ExactlyOnceStateMachine::new(Box::new(
                MockStateMachine {commands: Mutex::new(sender)}))
    }
}

impl StateMachine for MockStateMachine {
    /// Copies buffer and sends it down the commands pipe
    ///
    /// #Panics
    /// Panics if the commands reciever has been deallocated
    fn command (&self, buffer: &[u8]) -> Result<(), RaftError> {
        let buf_clone: Vec<u8> = buffer
                                .iter()
                                .cloned()
                                .collect();
        println!("State machine received command {:?}", buf_clone.clone());
        self.commands.lock().unwrap().send(buf_clone.clone()).unwrap();
        Ok(())
    }

    /// query just drops the buffer for now
    fn query(&self, _: &[u8]) -> Result<Vec<u8>, RaftError> {
        Ok(Vec::new())
    }
}
