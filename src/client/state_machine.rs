use std::collections::HashMap;
use super::super::common::{RaftError, RaftCommandReply,
                           RaftQueryReply, RaftCommand, RaftQuery};

///
/// State machine trait for clients to implement. Client should define
/// their own deserialization/serialization for the |buffer| that
/// Raft passes around as an anonymous blob.
/// TODO : implement StateMachineError trait instead of using IoError
///
pub trait StateMachine: Sync + Send {
    /// 
    /// Perform the command defined by |buffer| on this state machine.
    ///
    /// # Returns
    /// Returns an Ok if command successfully executes; otherwise
    /// results in an IoError.
    ///
    fn command(&self, buffer: &[u8]) -> Result<(), RaftError>;

    ///
    /// Performs the query defined by |buffer| on this state machine.
    ///
    /// # Returns
    /// Returns a |buffer| (to be interpreted by client) wrapped in Result if
    /// query successfully executes. Otherwise, results in an IoError.
    ///
    fn query(&self, buffer: &[u8]) -> Result<Vec<u8>, RaftError>;
}

pub struct Session {
    last_updated: u64,
    last_outstanding_op: u64,
    responses: HashMap<u64, Result<(), RaftError>>,
}

pub struct ExactlyOnceStateMachine {
    // client state machine
    client_state_machine: Box<StateMachine>,
    // Client ID to session.
    sessions: HashMap<u64, Session>
}

impl ExactlyOnceStateMachine {
    pub fn new(client_state_machine: Box<StateMachine>) -> ExactlyOnceStateMachine {
        ExactlyOnceStateMachine {
            client_state_machine: client_state_machine,
            sessions: HashMap::new(),
        }
    }

    pub fn new_session(&self) {
    }

    pub fn command (&self, data: &RaftCommand)
        -> Result<RaftCommandReply, RaftError> {
        match *data {
            RaftCommand::StateMachineCommand{ref data, session} => {
        // TODO (sydli) verify session
            self.client_state_machine.command(data)
                .map(|_| RaftCommandReply::StateMachineCommand)
            },
            RaftCommand::OpenSession => {
                // TODO (sydli) impl open session
                Ok(RaftCommandReply::Noop)
            },
            _ => Ok(RaftCommandReply::Noop),
        }
    }

    pub fn query (&self, data: &RaftQuery) -> Result<RaftQueryReply, RaftError> {
        match *data {
            RaftQuery::StateMachineQuery(ref buffer) =>
                self.client_state_machine.query(buffer)
                    .map(RaftQueryReply::StateMachineQuery),
            _ => Err(RaftError::Unknown),
        }
    }
}

