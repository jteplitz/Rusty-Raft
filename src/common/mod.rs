pub mod constants;
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;
use std::io::{Error as IoError};
use raft_capnp::{session_info};

#[derive(Debug, Clone, PartialEq)]
pub enum RaftError { 
    ClientError(String),      // Error defined by client.
    NotLeader(Option<u64>),   // I'm not the leader; give leader's id
    IoError(String),
                              // if we know it.
    RpcError(String),
    Timeout,
    SessionError,
    Unknown,
}

// TODO: None of the deserialization in this file should call unwrap
/// 
/// High-level abstraction for message types:
///
/// `client_command` Request/Reply is the input/output for Raft's client
/// handler (the message between |client| and the Raft cluster).
///
/// `raft_command` and `raft_query` Request/Reply are input/output types for
/// |RaftStateMachine::command| and |RaftStateMachine::query|, respectively.
///
/// The sub-types of the above, Request/Reply::StateMachine, are input/output
/// types for the client's StateMachine command and query.
///
/// Changes to any of these messages should be reflected in the equivalent
/// protocols in protocol/raft.capnp, and in the appropriate proto serialization
/// functions.
///

///
/// `raft_command` Request/Reply are input/output types for |RaftStateMachine::command|
///
pub mod raft_command {
    use super::{SessionInfo, raft_server};
    use super::super::raft_capnp::raft_command as proto;
    use std::net::SocketAddr;

    #[derive(Clone, Debug, PartialEq)]
    pub enum Request {
        StateMachineCommand { data: Vec<u8>, session: SessionInfo },
        OpenSession (u64),
        SetConfig (Vec<(u64, SocketAddr)>),
        Noop,
    }

    #[derive(Clone, Debug, PartialEq)]
    pub enum Reply {
        StateMachineCommand,
        OpenSession,
        SetConfig,
        Noop,
    }

    #[cfg(test)]
    pub fn dummy_request() -> Request {
        let data = vec![10, 35, 6];
        let session = SessionInfo {
            client_id: 0,
            sequence_number: 0,
        };
        Request::StateMachineCommand {data: data, session: session}
    }

    #[cfg(test)]
    pub fn dummy_reply() -> Reply {
        Reply::OpenSession
    }

    /// 
    /// Generates corresponding Reply for Request.
    ///
    #[cfg(test)]
    pub fn successful_reply_for(request: Request) -> Reply {
        match request {
            Request::StateMachineCommand{..} => 
                Reply::StateMachineCommand,
            Request::OpenSession(_) => Reply::OpenSession,
            Request::SetConfig(_) => Reply::SetConfig,
            Request::Noop => Reply::Noop
        }
    }

    /// 
    /// Serialization to and from proto.
    ///
    pub fn request_to_proto(command: Request, builder: &mut proto::Builder){
        match command {
            Request::StateMachineCommand { data, session } => {
                let mut command = builder.borrow().init_state_machine_command();
                command.set_data(&data);
                session.into_proto(&mut command.init_session());
            },
            Request::OpenSession(client_id) => builder.set_open_session(client_id),
            Request::SetConfig(servers) => {
                let mut proto_servers = builder.borrow().init_set_config(servers.len() as u32);
                for (i, server) in servers.into_iter().enumerate() {
                    let server_builder = proto_servers.borrow().get(i as u32);
                    raft_server::to_proto(server, server_builder);
                }
            }
            Request::Noop => builder.set_noop(()),
        }
    }

    // TODO: Should return a result instead of unwrapping
    pub fn request_from_proto(proto: proto::Reader) -> Request {
        match proto.which().unwrap() {
            proto::StateMachineCommand(command) => {
                let command = command.unwrap();
                Request::StateMachineCommand {
                   data: command.get_data().unwrap().to_vec(),
                   session: SessionInfo::from_proto(command.get_session().unwrap()),
                }
            },
            proto::OpenSession(client_id) => Request::OpenSession(client_id),
            proto::SetConfig(config) => {
                let servers = config.unwrap().iter()
                    .map(|s| raft_server::from_proto(s).unwrap())
                    .collect::<Vec<(u64, SocketAddr)>>();

                Request::SetConfig(servers)
            },
            proto::Noop(_) => Request::Noop,
        }
    }

    pub fn reply_to_proto(reply: Reply, builder: &mut proto::reply::Builder) {
        match reply {
            Reply::StateMachineCommand => builder.set_state_machine_command(()),
            Reply::OpenSession => builder.set_open_session(()),
            Reply::SetConfig => builder.set_set_config(()),
            Reply::Noop => builder.set_noop(()),
        }
    }

    pub fn reply_from_proto(proto: &mut proto::reply::Reader) -> Reply {
        match proto.which().unwrap() {
            proto::reply::StateMachineCommand(_) => Reply::StateMachineCommand,
            proto::reply::OpenSession(_) => Reply::OpenSession,
            proto::reply::SetConfig(_) => Reply::SetConfig,
            proto::reply::Noop(_) => Reply::Noop,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{request_to_proto, request_from_proto,
                    reply_to_proto, reply_from_proto,
                    dummy_request, dummy_reply};
        use super::super::super::raft_capnp::{raft_command as proto};
        use super::super::super::rpc::client::Rpc;

        #[test]
        fn request_to_and_from_proto() {
            let mut rpc = Rpc::new(1);
            let request = dummy_request();
            {
                let mut builder = rpc.get_param_builder()
                                     .init_as::<proto::Builder>();
                request_to_proto(request.clone(), &mut builder);
            }
            let reader = rpc.get_param_builder().as_reader()
                            .get_as::<proto::Reader>().unwrap();
            assert_eq!(request, request_from_proto(reader));
        }

        #[test]
        fn reply_to_and_from_proto() {
            let mut rpc = Rpc::new(1);
            let reply = dummy_reply();
            {
                let mut builder = rpc.get_param_builder()
                                     .init_as::<proto::reply::Builder>();
                reply_to_proto(reply.clone(), &mut builder);
            }
            let mut reader = rpc.get_param_builder().as_reader()
                            .get_as::<proto::reply::Reader>().unwrap();
            assert_eq!(reply, reply_from_proto(&mut reader));
        }
    }
}

///
/// `raft_query` Request/Reply are input/output types for |RaftStateMachine::query|
///
pub mod raft_query {
    use super::super::raft_capnp::raft_query as proto;

    #[derive(Clone, Debug, PartialEq)]
    pub enum Request {
        StateMachineQuery ( Vec<u8> )
    }

    #[derive(Clone, Debug, PartialEq)]
    pub enum Reply {
        StateMachineQuery ( Vec<u8> )
    }

    #[cfg(test)]
    pub fn successful_reply_for(request: Request) -> Reply {
        match request {
            Request::StateMachineQuery(_) => Reply::StateMachineQuery(vec![])
        }
    }

    pub fn request_from_proto(proto: proto::Reader) -> Request {
        Request::StateMachineQuery(proto.get_state_machine_query().unwrap().to_vec())
    }

    pub fn request_to_proto(query: Request, builder: &mut proto::Builder){
        match query {
            Request::StateMachineQuery(data) => {
                builder.set_state_machine_query(&data);
            }
        }
    }

    pub fn reply_to_proto(reply: Reply, builder: &mut proto::reply::Builder) {
        match reply {
            Reply::StateMachineQuery(data) => {
                builder.set_state_machine_query(&data);
            }
        }
    }

    pub fn reply_from_proto(proto: &mut proto::reply::Reader) -> Reply {
        Reply::StateMachineQuery(proto.get_state_machine_query().unwrap().to_vec())
    }

    #[cfg(test)]
    mod tests {
        use super::{Request, Reply,
                    request_to_proto, request_from_proto,
                    reply_to_proto, reply_from_proto};
        use super::super::super::raft_capnp::{raft_query as proto};
        use super::super::super::rpc::client::Rpc;

        pub fn dummy_request() -> Request {
            let data = vec![10, 35, 6];
            Request::StateMachineQuery (data.to_vec())
        }

        pub fn dummy_reply() -> Reply {
            let data = vec![10, 35, 6];
            Reply::StateMachineQuery (data.to_vec())
        }

        #[test]
        fn request_to_and_from_proto() {
            let mut rpc = Rpc::new(1);
            let request = dummy_request();
            {
                let mut builder = rpc.get_param_builder()
                                     .init_as::<proto::Builder>();
                request_to_proto(request.clone(), &mut builder);
            }
            let reader = rpc.get_param_builder().as_reader()
                            .get_as::<proto::Reader>().unwrap();
            assert_eq!(request, request_from_proto(reader));
        }

        #[test]
        fn reply_to_and_from_proto() {
            let mut rpc = Rpc::new(1);
            let reply = dummy_reply();
            {
                let mut builder = rpc.get_param_builder()
                                     .init_as::<proto::reply::Builder>();
                reply_to_proto(reply.clone(), &mut builder);
            }
            let mut reader = rpc.get_param_builder().as_reader()
                            .get_as::<proto::reply::Reader>().unwrap();
            assert_eq!(reply, reply_from_proto(&mut reader));
        }
    }
}

/// Utility functions for serializing and deserializing a raft_server
mod raft_server {
    use super::super::raft_capnp::raft_server as proto;
    use capnp::{Result, Error, ErrorKind};
    use std::net::{SocketAddr};
    use std::str::FromStr;

    /// Deserializes a raft server from a proto into an id socket_addr tuple
    pub fn from_proto(proto: proto::Reader) -> Result<(u64, SocketAddr)> {
        Ok((proto.get_id(), deserialize_addr(proto.get_addr()?)?))
    }

    pub fn to_proto(server: (u64, SocketAddr), mut proto: proto::Builder) {
        proto.set_id(server.0);
        proto.set_addr(&serialize_addr(server.1));
    }


    fn deserialize_addr(addr: &str) -> Result<SocketAddr> {
        SocketAddr::from_str(addr)
            .map_err(|_| Error {kind: ErrorKind::Failed, description: String::from("Invalid address for raft server")})
    }

    fn serialize_addr(addr: SocketAddr) -> String {
        addr.to_string()
    }
}

///
/// `client_command` Request/Reply are for the RPC between main thread's
/// |ClientHandler| and the client's |RaftConnection|, which pass ClientRequest
/// protobufs between each other.
///
pub mod client_command {
    use super::{raft_command, raft_query, raft_server, RaftError};
    use super::super::raft_capnp::{client_request as proto, raft_error, not_leader};
    use std::net::SocketAddr;

    #[derive(Clone, Debug, PartialEq)]
    pub enum Request {
        Command(raft_command::Request),
        Query(raft_query::Request),
        AddServer((u64, SocketAddr)),
        RemoveServer((u64, SocketAddr))
    }

    #[derive(Clone, Debug, PartialEq)]
    pub enum Reply {
        Command(raft_command::Reply),
        Query(raft_query::Reply),
        AddServer,
        RemoveServer
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
            proto::AddServer(raft_server) => {
                Request::AddServer(
                    raft_server::from_proto(raft_server.unwrap()).unwrap())
            },
            proto::RemoveServer(raft_server) => {
                Request::RemoveServer(
                    raft_server::from_proto(raft_server.unwrap()).unwrap())
            }
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
                    Reply::AddServer => builder.set_add_server_reply(()),
                    Reply::RemoveServer => builder.set_remove_server_reply(())
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
            Request::Command(data) =>
                Reply::Command(raft_command::successful_reply_for(data)),
            Request::Query(data) =>
                Reply::Query(raft_query::successful_reply_for(data)),
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
            Request::AddServer(raft_server) => {
                raft_server::to_proto(raft_server, builder.borrow().init_add_server());
            },
            Request::RemoveServer(raft_server) => {
                raft_server::to_proto(raft_server, builder.borrow().init_remove_server());
            }
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
            proto::reply::AddServerReply(_) => Ok(Reply::AddServer),
            proto::reply::RemoveServerReply(_) => Ok(Reply::RemoveServer),
        }
    }

    fn raft_error_to_proto(err: RaftError, builder: &mut raft_error::Builder) {
        match err {
            RaftError::ClientError(err) => builder.set_client_error(err.as_str()),
            RaftError::IoError(err) => builder.set_io_error(err.as_str()),
            RaftError::NotLeader(leader) => {
                match leader {
                    Some(id) => {
                        builder.borrow().init_not_leader().set_leader_id(id);
                    },
                    None => {
                        builder.borrow().init_not_leader().set_leader_unknown(());
                    }
                }
            },
            RaftError::SessionError => builder.set_session_error(()),
            RaftError::RpcError(_) | RaftError::Unknown => builder.set_unknown(()),
            RaftError::Timeout => builder.set_timeout(())
        }
    }

    fn raft_error_from_proto(proto: &mut raft_error::Reader) -> RaftError {
        match proto.which().unwrap() {
            raft_error::ClientError(err) => RaftError::ClientError(
                err.unwrap().to_string()),
            raft_error::NotLeader(leader) => {
                let leader_id = match leader.unwrap().which().unwrap() {
                    not_leader::LeaderId(id) => Some(id),
                    not_leader::LeaderUnknown(_) => None
                };
                RaftError::NotLeader(leader_id)
            }
            raft_error::SessionError(_) => RaftError::SessionError,
            raft_error::IoError(err) => RaftError::IoError(err.unwrap().to_string()),
            raft_error::Unknown(_) => RaftError::Unknown,
            raft_error::Timeout(_) => RaftError::Timeout
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{Request, Reply,
                    request_to_proto, request_from_proto,
                    reply_to_proto, reply_from_proto};
        use super::super::{RaftError, raft_command};
        use super::super::super::raft_capnp::{client_request as proto};
        use super::super::super::rpc::client::Rpc;
        use std::string::String;

        fn dummy_request() -> Request {
            Request::Command(raft_command::dummy_request())
        }

        fn dummy_reply() -> Result<Reply, RaftError> {
            Ok(Reply::Command(raft_command::dummy_reply()))
        }

        fn dummy_error_reply() -> Result<Reply, RaftError> {
            let s = String::from("This is a dumbo error");
            Err(RaftError::ClientError(s))
        }

        #[test]
        fn request_to_and_from_proto() {
            let mut rpc = Rpc::new(1);
            let request = dummy_request();
            {
                let mut builder = rpc.get_param_builder()
                                     .init_as::<proto::Builder>();
                request_to_proto(request.clone(), &mut builder);
            }
            let reader = rpc.get_param_builder().as_reader()
                            .get_as::<proto::Reader>().unwrap();
            assert_eq!(request, request_from_proto(reader));
        }

        #[test]
        fn reply_to_and_from_proto() {
            let mut rpc = Rpc::new(1);
            let reply = dummy_reply();
            {
                let mut builder = rpc.get_param_builder()
                                     .init_as::<proto::reply::Builder>();
                reply_to_proto(reply.clone(), &mut builder);
            }
            let mut reader = rpc.get_param_builder().as_reader()
                            .get_as::<proto::reply::Reader>().unwrap();
            assert_eq!(reply.unwrap(), reply_from_proto(&mut reader).unwrap());
        }

        #[test]
        fn error_reply_to_and_from_proto() {
            let mut rpc = Rpc::new(1);
            let reply = dummy_error_reply();
            {
                let mut builder = rpc.get_param_builder()
                                     .init_as::<proto::reply::Builder>();
                reply_to_proto(reply.clone(), &mut builder);
            }
            let mut reader = rpc.get_param_builder().as_reader()
                            .get_as::<proto::reply::Reader>().unwrap();
            assert_eq!(reply.unwrap_err(), reply_from_proto(&mut reader).unwrap_err());
        }

    }
}

/// 
/// Common Config object to specify initial cluster configuration and other
/// global variables.
///
pub struct Config<'a> {
    // Each server has a unique 64bit integer id that and a socket address
    // These mappings MUST be identical for each server in the cluster
    pub me: (u64, SocketAddr),
    pub heartbeat_timeout: Duration,
    pub state_filename: &'a str,
    pub log_filename: &'a str
}

impl<'a> Config<'a> {
    pub fn new<A: ToSocketAddrs> (my_id: u64, my_addr: A, heartbeat_timeout: Duration,
                state_filename: &'a str, log_filename: &'a str) -> Config<'a> {
        Config {
            me: (my_id, my_addr.to_socket_addrs().unwrap().next().unwrap()),
            heartbeat_timeout: heartbeat_timeout,
            state_filename: state_filename,
            log_filename: log_filename
        }
    }
}

///
/// Client session info object to pass along with commands.
///
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SessionInfo {
    pub client_id: u64,
    pub sequence_number: u64,
}

///
/// Creates a default/mock/test session.
///
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

