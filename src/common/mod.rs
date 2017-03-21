pub mod constants;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use raft_capnp::{session_info, client_request, raft_command, raft_query, raft_error};
use rpc::RpcError;

#[derive(Debug)]
pub enum RaftError { 
    ClientError(String),      // Error defined by client.
    NotLeader(Option<SocketAddr>),   // I'm not the leader; give leader id
                              // if we know it.
                              // TODO: actually keep track of the leader
    RpcError(RpcError),
    SessionError,
    Unknown,
}

///
/// Changes to ClientRequest should be reflected in the
/// equivalent protocols in protocol/raft.capnp
///
#[derive(Clone, Debug, PartialEq)]
pub enum RaftCommand {
    StateMachineCommand { data: Vec<u8>, session: SessionInfo },
    OpenSession,
    SetConfig,
    Noop,
}

#[derive(Clone, Debug)]
pub enum RaftCommandReply {
    StateMachineCommand,
    OpenSession (u64),
    SetConfig,
    Noop,
}

#[derive(Clone, Debug)]
pub enum RaftQuery {
    StateMachineQuery ( Vec<u8> ),
    GetConfig,
}

#[derive(Clone, Debug)]
pub enum RaftQueryReply {
    StateMachineQuery ( Vec<u8> ),
    GetConfig ( Vec<u8> ),
}

#[derive(Clone, Debug)]
pub enum ClientRequest {
    Command(RaftCommand),
    Query(RaftQuery),
    Noop, // TODO (sydli) delete
    Unknown,
}

#[derive(Clone, Debug)]
pub enum ClientRequestReply {
    Command(RaftCommandReply),
    Query(RaftQueryReply),
}

#[cfg(test)]
pub fn successful_reply_for(op: ClientRequest) -> ClientRequestReply {
    match op {
        ClientRequest::Command(data) => {
            ClientRequestReply::Command(
                match data {
                    RaftCommand::StateMachineCommand{..} => 
                        RaftCommandReply::StateMachineCommand,
                    RaftCommand::OpenSession => 
                        RaftCommandReply::OpenSession(0),
                    RaftCommand::SetConfig => RaftCommandReply::SetConfig,
                    RaftCommand::Noop => RaftCommandReply::Noop
                }
            )
        },
        ClientRequest::Query(data) => {
            ClientRequestReply::Query(
                match data {
                    RaftQuery::StateMachineQuery(_) =>
                        RaftQueryReply::StateMachineQuery(vec![]),
                    RaftQuery::GetConfig => RaftQueryReply::GetConfig(vec![]),
                }
            )
        },
        _ => ClientRequestReply::Command(RaftCommandReply::Noop),
    }
}

pub fn raft_command_from_proto(proto: raft_command::Reader) -> RaftCommand {
    match proto.which().unwrap() {
        raft_command::StateMachineCommand(command) => {
            let command = command.unwrap();
            RaftCommand::StateMachineCommand {
               data: command.get_data().unwrap().to_vec(),
               session: SessionInfo::from_proto(command.get_session().unwrap()),
            }
        },
        raft_command::OpenSession(_) => RaftCommand::OpenSession,
        raft_command::SetConfig(_) => RaftCommand::SetConfig,
        raft_command::Noop(_) => RaftCommand::Noop,
    }
}

fn raft_query_from_proto(proto: raft_query::Reader) -> RaftQuery {
    match proto.which().unwrap() {
        raft_query::StateMachineQuery(query) => {
            RaftQuery::StateMachineQuery(query.unwrap().to_vec())
        },
        raft_query::GetConfig(_) => RaftQuery::GetConfig,
    }
}

pub fn client_request_from_proto(proto: client_request::Reader) -> ClientRequest {
    match proto.which().unwrap() {
        client_request::Command(raft_command) => {
            ClientRequest::Command(
                raft_command_from_proto(raft_command.unwrap()))
        },
        client_request::Query(raft_query) => {
            ClientRequest::Query(
                raft_query_from_proto(raft_query.unwrap()))
        },
        client_request::Noop(_) => ClientRequest::Noop,
        client_request::Unknown(_) => ClientRequest::Unknown,
    }
}

fn raft_query_to_proto(query: RaftQuery, builder: &mut raft_query::Builder){
    match query {
        RaftQuery::StateMachineQuery(data) => {
            builder.set_state_machine_query(&data);
        },
        RaftQuery::GetConfig => builder.set_get_config(()),
    }
}

pub fn raft_command_to_proto(command: RaftCommand, builder: &mut raft_command::Builder){
    match command {
        RaftCommand::StateMachineCommand { data, session } => {
            let mut command = builder.borrow().init_state_machine_command();
            command.set_data(&data);
            session.into_proto(&mut command.init_session());
        },
        RaftCommand::OpenSession => builder.set_open_session(()),
        RaftCommand::SetConfig => builder.set_set_config(()),
        RaftCommand::Noop => builder.set_noop(()),
    }
}

fn raft_query_reply_to_proto(reply: RaftQueryReply,
                             builder: &mut raft_query::reply::Builder) {
    match reply {
        RaftQueryReply::StateMachineQuery(data) => builder.set_state_machine_query(&data),
        RaftQueryReply::GetConfig(data) => builder.set_get_config(&data),
    }
}

fn raft_command_reply_to_proto(reply: RaftCommandReply,
                               builder: &mut raft_command::reply::Builder) {
    match reply {
        RaftCommandReply::StateMachineCommand => builder.set_state_machine_command(()),
        RaftCommandReply::OpenSession(client_id) => builder.set_open_session(client_id),
        RaftCommandReply::SetConfig => builder.set_set_config(()),
        RaftCommandReply::Noop => builder.set_noop(()),
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

pub fn client_request_reply_to_proto(op: Result<ClientRequestReply, RaftError>,
                                     builder: &mut client_request::reply::Builder) {
    match op {
        Ok(reply) => {
            match reply {
                ClientRequestReply::Command(command) =>
                    raft_command_reply_to_proto(command,
                                               &mut builder.borrow().init_command_reply()),
                ClientRequestReply::Query(query) => 
                    raft_query_reply_to_proto(query,
                                              &mut builder.borrow().init_query_reply()),
            }
        }, 
        Err(err) => {
            raft_error_to_proto(err, &mut builder.borrow().init_error());
        }
    }
}

fn raft_query_reply_from_proto(proto: &mut raft_query::reply::Reader) -> RaftQueryReply {
    match proto.which().unwrap() {
        raft_query::reply::StateMachineQuery(data) =>
            RaftQueryReply::StateMachineQuery(data.unwrap().to_vec()),
        raft_query::reply::GetConfig(data) =>
            RaftQueryReply::GetConfig(data.unwrap().to_vec()),
    }
}

fn raft_command_reply_from_proto(proto: &mut raft_command::reply::Reader) -> RaftCommandReply {
    match proto.which().unwrap() {
        raft_command::reply::StateMachineCommand(_) =>
            RaftCommandReply::StateMachineCommand,
        raft_command::reply::OpenSession(client_id) =>
            RaftCommandReply::OpenSession(client_id),
        raft_command::reply::SetConfig(_) =>
            RaftCommandReply::SetConfig,
        raft_command::reply::Noop(_) =>
            RaftCommandReply::Noop,
    }
}

fn raft_error_from_proto(proto: &mut raft_error::Reader) -> RaftError {
    match proto.which().unwrap() {
        raft_error::ClientError(err) => RaftError::ClientError(err.unwrap().to_string()),
        raft_error::NotLeader(leader) =>
            RaftError::NotLeader(
                leader.ok().and_then(|x| SocketAddr::from_str(x).ok())),
        raft_error::SessionError(_) => RaftError::SessionError,
        raft_error::Unknown(_) => RaftError::Unknown,
    }
}

pub fn client_request_reply_from_proto(proto: &mut client_request::reply::Reader)
    -> Result<ClientRequestReply, RaftError> {
    match proto.which().unwrap() {
        client_request::reply::Error(err) =>
            Err(raft_error_from_proto(&mut err.unwrap())),
        client_request::reply::CommandReply(command) =>
            Ok(ClientRequestReply::Command(
                    raft_command_reply_from_proto(&mut command.unwrap()))),
        client_request::reply::QueryReply(query) =>
            Ok(ClientRequestReply::Query(
                    raft_query_reply_from_proto(&mut query.unwrap()))),
    }
}

pub fn client_request_to_proto(op: ClientRequest, builder: &mut client_request::Builder){
    match op {
        ClientRequest::Command(raft_command) => {
            raft_command_to_proto(raft_command,
                                  &mut builder.borrow().init_command());
        },
        ClientRequest::Query(raft_query) => {
            raft_query_to_proto(raft_query,
                                &mut builder.borrow().init_query());
        },
        ClientRequest::Noop => builder.set_noop(()),
        ClientRequest::Unknown => builder.set_unknown(()),
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

