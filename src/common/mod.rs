pub mod constants;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use raft_capnp::{session_info, client_request, raft_error};
use rpc::RpcError;

#[derive(Debug, Clone)]
pub enum RaftError { 
    ClientError(String),      // Error defined by client.
    NotLeader(Option<SocketAddr>),   // I'm not the leader; give leader id
                              // if we know it.
                              // TODO: actually keep track of the leader
    RpcError(String),
    SessionError,
    Unknown,
}

pub mod raft_command {
    use super::SessionInfo;
    use super::super::raft_capnp::raft_command as proto;

    ///
    /// Changes to ClientRequest should be reflected in the
    /// equivalent protocols in protocol/raft.capnp
    ///
    #[derive(Clone, Debug, PartialEq)]
    pub enum Request {
        StateMachineCommand { data: Vec<u8>, session: SessionInfo },
        OpenSession,
        SetConfig,
        Noop,
    }

    #[derive(Clone, Debug)]
    pub enum Reply {
        StateMachineCommand,
        OpenSession (u64),
        SetConfig,
        Noop,
    }

    pub fn request_to_proto(command: Request, builder: &mut proto::Builder){
        match command {
            Request::StateMachineCommand { data, session } => {
                let mut command = builder.borrow().init_state_machine_command();
                command.set_data(&data);
                session.into_proto(&mut command.init_session());
            },
            Request::OpenSession => builder.set_open_session(()),
            Request::SetConfig => builder.set_set_config(()),
            Request::Noop => builder.set_noop(()),
        }
    }

    pub fn request_from_proto(proto: proto::Reader) -> Request {
        match proto.which().unwrap() {
            proto::StateMachineCommand(command) => {
                let command = command.unwrap();
                Request::StateMachineCommand {
                   data: command.get_data().unwrap().to_vec(),
                   session: SessionInfo::from_proto(command.get_session().unwrap()),
                }
            },
            proto::OpenSession(_) => Request::OpenSession,
            proto::SetConfig(_) => Request::SetConfig,
            proto::Noop(_) => Request::Noop,
        }
    }


    pub fn reply_to_proto(reply: Reply,
                                   builder: &mut proto::reply::Builder) {
        match reply {
            Reply::StateMachineCommand => builder.set_state_machine_command(()),
            Reply::OpenSession(client_id) => builder.set_open_session(client_id),
            Reply::SetConfig => builder.set_set_config(()),
            Reply::Noop => builder.set_noop(()),
        }
    }

    pub fn reply_from_proto(proto: &mut proto::reply::Reader) -> Reply {
        match proto.which().unwrap() {
            proto::reply::StateMachineCommand(_) => Reply::StateMachineCommand,
            proto::reply::OpenSession(client_id) => Reply::OpenSession(client_id),
            proto::reply::SetConfig(_) => Reply::SetConfig,
            proto::reply::Noop(_) => Reply::Noop,
        }
    }
}
pub mod raft_query {
    use super::SessionInfo;
    use super::super::raft_capnp::raft_query as proto;

    #[derive(Clone, Debug)]
    pub enum Request {
        StateMachineQuery ( Vec<u8> ),
        GetConfig,
    }

    #[derive(Clone, Debug)]
    pub enum Reply {
        StateMachineQuery ( Vec<u8> ),
        GetConfig ( Vec<u8> ),
    }

    pub fn request_from_proto(proto: proto::Reader) -> Request {
        match proto.which().unwrap() {
            proto::StateMachineQuery(query) => {
                Request::StateMachineQuery(query.unwrap().to_vec())
            },
            proto::GetConfig(_) => Request::GetConfig,
        }
    }

    pub fn request_to_proto(query: Request, builder: &mut proto::Builder){
        match query {
            Request::StateMachineQuery(data) => {
                builder.set_state_machine_query(&data);
            },
            Request::GetConfig => builder.set_get_config(()),
        }
    }

    pub fn reply_to_proto(reply: Reply, builder: &mut proto::reply::Builder) {
        match reply {
            Reply::StateMachineQuery(data) => builder.set_state_machine_query(&data),
            Reply::GetConfig(data) => builder.set_get_config(&data),
        }
    }

    pub fn reply_from_proto(proto: &mut proto::reply::Reader) -> Reply {
        match proto.which().unwrap() {
            proto::reply::StateMachineQuery(data) =>
                Reply::StateMachineQuery(data.unwrap().to_vec()),
            proto::reply::GetConfig(data) =>
                Reply::GetConfig(data.unwrap().to_vec()),
        }
    }
}

pub mod client_command {
    use super::{raft_command, raft_query, RaftError};
    use super::super::raft_capnp::{client_request as proto, raft_error};
    use std::net::SocketAddr;
    use std::str::FromStr;

    #[derive(Clone, Debug)]
    pub enum Request {
        Command(raft_command::Request),
        Query(raft_query::Request),
        Unknown,
    }

    #[derive(Clone, Debug)]
    pub enum Reply {
        Command(raft_command::Reply),
        Query(raft_query::Reply),
    }

    pub fn request_from_proto(proto: proto::Reader) -> Request {
        match proto.which().unwrap() {
            proto::Command(raft_command) => {
                Request::Command(
                    raft_command::request_from_proto(raft_command.unwrap()))
            },
            proto::Query(raft_query) => {
                Request::Query(
                    raft_query::request_from_proto(raft_query.unwrap()))
            },
            proto::Unknown(_) => Request::Unknown,
        }
    }

    pub fn reply_to_proto(op: Result<Reply, RaftError>,
                                         builder: &mut proto::reply::Builder) {
        match op {
            Ok(reply) => {
                match reply {
                    Reply::Command(command) =>
                        raft_command::reply_to_proto(
                            command, &mut builder.borrow().init_command_reply()),
                    Reply::Query(query) => 
                        raft_query::reply_to_proto(
                            query, &mut builder.borrow().init_query_reply()),
                }
            }, 
            Err(err) => {
                raft_error_to_proto(err, &mut builder.borrow().init_error());
            }
        }
    }

    #[cfg(test)]
    pub fn successful_reply_for(op: Request) -> Reply {
        match op {
            Request::Command(data) => {
                Reply::Command(
                    match data {
                        raft_command::Request::StateMachineCommand{..} => 
                            raft_command::Reply::StateMachineCommand,
                        raft_command::Request::OpenSession => 
                            raft_command::Reply::OpenSession(0),
                        raft_command::Request::SetConfig => raft_command::Reply::SetConfig,
                        raft_command::Request::Noop => raft_command::Reply::Noop
                    }
                )
            },
            Request::Query(data) => {
                Reply::Query(
                    match data {
                        raft_query::Request::StateMachineQuery(_) =>
                            raft_query::Reply::StateMachineQuery(vec![]),
                        raft_query::Request::GetConfig => raft_query::Reply::GetConfig(vec![]),
                    }
                )
            },
            _ => Reply::Command(raft_command::Reply::Noop),
        }
    }

    pub fn request_to_proto(op: Request, builder: &mut proto::Builder){
        match op {
            Request::Command(raft_command) => {
                raft_command::request_to_proto(raft_command,
                                      &mut builder.borrow().init_command());
            },
            Request::Query(raft_query) => {
                raft_query::request_to_proto(raft_query,
                                    &mut builder.borrow().init_query());
            },
            Request::Unknown => builder.set_unknown(()),
        }
    }

    pub fn reply_from_proto(proto: &mut proto::reply::Reader)
        -> Result<Reply, RaftError> {
        match proto.which().unwrap() {
            proto::reply::Error(err) =>
                Err(raft_error_from_proto(&mut err.unwrap())),
            proto::reply::CommandReply(command) =>
                Ok(Reply::Command(
                        raft_command::reply_from_proto(&mut command.unwrap()))),
            proto::reply::QueryReply(query) =>
                Ok(Reply::Query(
                        raft_query::reply_from_proto(&mut query.unwrap()))),
        }
    }

    fn raft_error_to_proto(err: RaftError, builder: &mut raft_error::Builder) {
        match err {
            RaftError::ClientError(err) => builder.set_client_error(err.as_str()),
            RaftError::NotLeader(leader) => {
                let leader_str = leader.map(|x| x.to_string())
                                       .unwrap_or_default();
                builder.set_not_leader(leader_str.as_str())
            },
            RaftError::SessionError => builder.set_session_error(()),
            RaftError::RpcError(_) | RaftError::Unknown => builder.set_unknown(()),
        }
    }

    fn raft_error_from_proto(proto: &mut raft_error::Reader) -> RaftError {
        match proto.which().unwrap() {
            raft_error::ClientError(err) => RaftError::ClientError(
                err.unwrap().to_string()),
            raft_error::NotLeader(leader) =>
                RaftError::NotLeader(
                    leader.ok().and_then(|x| SocketAddr::from_str(x).ok())),
            raft_error::SessionError(_) => RaftError::SessionError,
            raft_error::Unknown(_) => RaftError::Unknown,
        }
    }
}

pub struct Config {
    // Each server has a unique 64bit integer id that and a socket address
    // These mappings MUST be identical for each server in the cluster
    pub cluster: HashMap<u64, SocketAddr>,
    pub me: (u64, SocketAddr),
    pub heartbeat_timeout: Duration,
    pub state_filename: String
}

impl Config {
    pub fn new (cluster: HashMap<u64, SocketAddr>, my_id: u64,
                my_addr: SocketAddr, heartbeat_timeout: Duration, state_filename: String) -> Config {
        Config {
            cluster: cluster,
            me: (my_id, my_addr),
            heartbeat_timeout: heartbeat_timeout,
            state_filename: state_filename
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SessionInfo {
    pub client_id: u64,
    pub sequence_number: u64,
}

pub fn mock_session() -> SessionInfo {
    SessionInfo {
        client_id:0,
        sequence_number:0
    }
}

impl SessionInfo {

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

