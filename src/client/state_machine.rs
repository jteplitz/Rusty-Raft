use std::collections::HashMap;
use std::time::{Instant};
use super::super::common::{RaftError, raft_command, raft_query, SessionInfo};

///
/// State machine trait for clients to implement. Client should define
/// their own deserialization/serialization for the |buffer| that
/// Raft passes around as an anonymous blob.
///
pub trait StateMachine:  Send {
    /// 
    /// Perform the command defined by |buffer| on this state machine.
    ///
    /// # Returns
    /// Results in Ok if command successfully executes; otherwise
    /// a RaftError::ClientError.
    ///
    fn command(&mut self, buffer: &[u8]) -> Result<(), RaftError>;

    ///
    /// Performs the query defined by |buffer| on this state machine.
    ///
    /// # Returns
    /// Returns a |buffer| (to be interpreted by client) if query
    /// successfully executes. Otherwise, results in RaftError::ClientError.
    ///
    fn query(&self, buffer: &[u8]) -> Result<Vec<u8>, RaftError>;
}


///
/// A Session for a single client. Caches all responses to client commands
/// to ensure each command is executed exactly once.
///
#[derive(Clone, Debug)]
pub struct Session {
    // Last time this Session was touched. TODO (sydli) If it's expired, remove it.
    last_update: Instant,
    // A map of sequence_number to generated command replies.
    responses: HashMap<u64, Result<raft_command::Reply, RaftError>>,
}

///
/// Abstraction over |StateMachine| that ensures |command| is not called
/// twice on the same data.
///
/// Requires clients to register sessions via
/// RaftCommand::OpenSession, and each command buffer to be accompanied by
/// SessionInfo.
///
/// Queries are sessionless.
///
/// Usage:
/// 
/// ```rust
/// # use rusty_raft::common::{raft_command, SessionInfo, RaftError};
/// # use rusty_raft::client::state_machine::{RaftStateMachine, StateMachine};
/// # pub struct EmptyStateMachine {}
/// # impl StateMachine for EmptyStateMachine {
/// #     fn command(&mut self, _: &[u8]) -> Result<(), RaftError> {Ok(())}
/// #     fn query(& self, _: &[u8]) -> Result<Vec<u8>, RaftError> {Ok(vec![])}
/// # }
/// # let mut state_machine = RaftStateMachine::new(Box::new(EmptyStateMachine{}));
/// let session_id = 5; // normally you'd randomly generate this
/// state_machine.command(&raft_command::Request::OpenSession(session_id)).unwrap();
/// let session_info = SessionInfo { client_id: session_id,
///                                  sequence_number: 0 };
/// state_machine.command(
///     &raft_command::Request::StateMachineCommand {
///         data: vec![],
///         session: session_info }).unwrap();
/// ```
pub struct RaftStateMachine {
    // State machine that we're wrapping.
    client_state_machine: Box<StateMachine>,
    // Maps client_id to a Session.
    sessions: HashMap<u64, Session>
}

impl RaftStateMachine {
    ///
    /// Creates a new linearizable state machine around |client_state_machine|.
    ///
    pub fn new(client_state_machine: Box<StateMachine>) -> RaftStateMachine {
        let mut sessions = HashMap::new();
        // NOTE: CLIENT ID OF 0 SHOULD BE USED FOR SESSIONLESS TESTING ONLY
        sessions.insert(0, Session {last_update: Instant::now(), responses: HashMap::new()});
        RaftStateMachine {
            client_state_machine: client_state_machine,
            sessions: sessions,
        }
    }

    ///
    /// Helper to generate a new session and update |sessions|.
    ///
    // TODO(sydli): Expire sessions
    // TODO (sydli): Fix: sessions should be generated
    //                client-side
    fn new_session(&mut self, session_id: u64) -> Result<raft_command::Reply, RaftError> {
        // TODO (sydli) if session already exists, what do?
        self.sessions.insert(session_id, Session {
            last_update: Instant::now(),
            responses: HashMap::new(),
        });
        Ok(raft_command::Reply::OpenSession)
    }

    ///
    /// Helper to ensure this command is not duplicated, using |session_info|.
    ///
    /// # Errors
    /// If |session_info.client_id| doesn't map to a known client session, returns 
    /// RaftError::SessionError.
    /// Otherwise, if the client command resulted in a RaftError, forwards it as-is.
    ///
    fn command_exactly_once(&mut self, data: &[u8], session_info: SessionInfo)
        -> Result<raft_command::Reply, RaftError> {
        let reply = self.sessions.get(&session_info.client_id).cloned()
            .ok_or(RaftError::SessionError)
            .and_then(|session| {
                session.responses.get(&session_info.sequence_number).cloned()
                       .unwrap_or_else(|| {
                           self.client_state_machine.command(data)
                               .map(|_| raft_command::Reply::StateMachineCommand)})
            });
        self.sessions.get_mut(&session_info.client_id).map(|ref mut session| {
            session.responses.insert(session_info.sequence_number, reply.clone());
            session.last_update = Instant::now();
        });
        reply
    }

    ///
    /// |session_id.sequence_number| is a unique identifier for the command; if it
    /// has not been seen before, runs the command on the client state machine.
    /// Otherwise, does not duplicate the run and returns the cached Result from the
    /// previous run.
    ///
    pub fn command (&mut self, data: &raft_command::Request)
        -> Result<raft_command::Reply, RaftError> {
        match *data {
            raft_command::Request::StateMachineCommand{ref data, session} =>
                self.command_exactly_once(data, session),
            raft_command::Request::OpenSession(client_id) =>
                self.new_session(client_id),
            _ => Ok(raft_command::Reply::Noop),
        }
    }

    ///
    /// Queries can be duplicated, so this simply forwards the query to the
    /// client state machine.
    ///
    pub fn query (&self, data: &raft_query::Request)
        -> Result<raft_query::Reply, RaftError> {
        match *data {
            raft_query::Request::StateMachineQuery(ref buffer) =>
                self.client_state_machine.query(buffer)
                    .map(raft_query::Reply::StateMachineQuery)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{StateMachine, RaftStateMachine};
    use super::super::super::common::{RaftError, raft_command, SessionInfo};
    use std::sync::mpsc::{channel, Sender};
    use rand;
    use rand::Rng;

    ///
    /// Dumb state machine that simply sends a message to |commands|
    /// each time |command| is called.
    ///
    /// Drops query requests.
    ///
    pub struct DumbStateMachine {
        commands: Option<Sender<()>>
    }

    impl StateMachine for DumbStateMachine {
        fn command (&mut self, _: &[u8]) -> Result<(), RaftError> {
            self.commands.clone().map(|x| x.send(()).unwrap());
            Ok(())
        }

        fn query(&self, _: &[u8]) -> Result<Vec<u8>, RaftError> {
            Ok(Vec::new())
        }
    }

    /// Helper to send |state_machine| an open session request.
    fn open_session(state_machine: &mut RaftStateMachine) -> u64 {
        let session_id = rand::thread_rng().next_u64();
        let reply = state_machine.command(
            &raft_command::Request::OpenSession(session_id))
                                 .unwrap();
        assert!(reply == raft_command::Reply::OpenSession);
        session_id
    }

    /// Helper to send |state_machine| an empty command with |session|.
    fn command_with_session(state_machine: &mut RaftStateMachine,
                            session: SessionInfo)
        -> Result<raft_command::Reply, RaftError> {
        state_machine.command(&raft_command::Request::StateMachineCommand {
            data: Vec::new(), session: session })
    }

    #[test]
    fn it_creates_a_session_successfully () {
        let client = Box::new(DumbStateMachine { commands: None });
        let mut state_machine = RaftStateMachine::new(client);
        open_session(&mut state_machine);
    }

    #[test]
    fn it_returns_session_error_on_incorrect_session () {
        let client = Box::new(DumbStateMachine { commands: None });
        let mut state_machine = RaftStateMachine::new(client);
        let client_id = open_session(&mut state_machine);
        let result = command_with_session(&mut state_machine,
                         SessionInfo {client_id: client_id + 1, sequence_number: 0 });
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RaftError::SessionError));
    }

    #[test]
    fn it_executes_command_exactly_once () {
        let (tx, rx) = channel();
        let client = Box::new(DumbStateMachine { commands: Some(tx.clone()) });
        let mut state_machine = RaftStateMachine::new(client);
        let client_id = open_session(&mut state_machine);
        assert!(command_with_session(&mut state_machine,
                    SessionInfo {client_id: client_id, sequence_number: 0 }).is_ok());
        assert!(rx.try_recv().is_ok());
        assert!(command_with_session(&mut state_machine,
                    SessionInfo {client_id: client_id, sequence_number: 0 }).is_ok());
        assert!(rx.try_recv().is_err());
        assert!(command_with_session(&mut state_machine,
                    SessionInfo {client_id: client_id, sequence_number: 1 }).is_ok());
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }
}
