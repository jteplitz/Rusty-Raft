use rand;
use rand::Rng;
use std::collections::HashMap;
use super::super::common::{RaftError, RaftCommandReply,
                           RaftQueryReply, RaftCommand, RaftQuery};

///
/// State machine trait for clients to implement. Client should define
/// their own deserialization/serialization for the |buffer| that
/// Raft passes around as an anonymous blob.
///
pub trait StateMachine: Sync + Send {
    /// 
    /// Perform the command defined by |buffer| on this state machine.
    ///
    /// # Returns
    /// Returns an Ok if command successfully executes; otherwise
    /// results in an IoError.
    ///
    fn command(&mut self, buffer: &[u8]) -> Result<(), RaftError>;

    ///
    /// Performs the query defined by |buffer| on this state machine.
    ///
    /// # Returns
    /// Returns a |buffer| (to be interpreted by client) wrapped in Result if
    /// query successfully executes. Otherwise, results in an IoError.
    ///
    fn query(&self, buffer: &[u8]) -> Result<Vec<u8>, RaftError>;
}

#[derive(Clone)]
pub struct Session {
    responses: HashMap<u64, Result<RaftCommandReply, RaftError>>,
}

pub struct ExactlyOnceStateMachine {
    // client state machine
    client_state_machine: Box<StateMachine>,
    // Client ID to session.
    sessions: HashMap<u64, Session>
}

impl ExactlyOnceStateMachine {
    pub fn new(client_state_machine: Box<StateMachine>) -> ExactlyOnceStateMachine {
        let mut sessions = HashMap::new();
        // NOTE: CLIENT ID OF 0 SHOULD BE USED FOR SESSIONLESS TESTING ONLY
        sessions.insert(0, Session {responses: HashMap::new()});
        ExactlyOnceStateMachine {
            client_state_machine: client_state_machine,
            sessions: sessions,
        }
    }

    fn get_session(&self, client_id: u64) -> Option<Session> {
        self.sessions.get(&client_id).cloned()
    }

    // TODO(sydli): Expire sessions
    fn new_session(&mut self) -> u64 {
        let session_id = rand::thread_rng().next_u64();
        self.sessions.insert(session_id, Session {
            responses: HashMap::new(),
        });
        session_id
    }

    pub fn command (&mut self, data: &RaftCommand)
        -> Result<RaftCommandReply, RaftError> {
        match *data {
            RaftCommand::StateMachineCommand{ref data, session} => {
                self.get_session(session.client_id)
                    .ok_or(RaftError::SessionError)
                    .and_then(|this_session| {
                        this_session.responses
                            .get(&session.sequence_number).cloned()
                            .unwrap_or_else(||
                                self.client_state_machine.command(data)
                                    .map(|_| RaftCommandReply::StateMachineCommand)
                                )
                   })
            },
            RaftCommand::OpenSession => {
                Ok(RaftCommandReply::OpenSession(self.new_session()))
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

