pub mod constants;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use raft_capnp::{session_info};
use rpc::{RpcError};

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

