use rpc::RpcError;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use raft_capnp::{session_info};

#[derive(Debug)]
pub enum RaftError { 
    ClientError(String),      // Error defined by client.
    NotLeader(Option<SocketAddr>),   // I'm not the leader; give leader id
                              // if we know it.
                              // TODO: actually keep track of the leader
    SessionError,
    RpcError(RpcError),
    Unknown,
}

pub struct Config {
    // Each server has a unique 64bit integer id that and a socket address
    // These mappings MUST be identical for eaah server in the cluster
    pub cluster: HashMap<u64, SocketAddr>,
    pub me: (u64, SocketAddr),
    pub heartbeat_timeout: Duration,
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
}

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

#[derive(Clone, Copy)]
pub struct SessionInfo {
    pub client_id: u64,
    pub sequence_number: u64,
}

impl SessionInfo {
    pub fn new_empty() -> SessionInfo{
        SessionInfo { client_id:0,
        sequence_number:0 }
    }

    pub fn from_proto(proto: session_info::Reader) -> SessionInfo {
        SessionInfo {
            client_id: proto.get_client_id(),
            sequence_number: proto.get_sequence_number(),
        }
    }

    pub fn into_proto(&self, builder: &mut session_info::Builder) {
        builder.set_client_id(self.client_id);
        builder.set_sequence_number(self.sequence_number);
    }
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

    pub fn command (&self, buffer: &[u8], session: SessionInfo)
        -> Result<(), RaftError> {
        self.client_state_machine.command(buffer);
        Ok(())
    }

    pub fn query (&self, buffer: &[u8]) -> Result<Vec<u8>, RaftError> {
        self.client_state_machine.query(buffer)
    }
}

