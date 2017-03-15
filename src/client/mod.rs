use capnp::serialize::OwnedSegments;
use capnp::message::Reader;
use raft_capnp::{client_request, client_request_reply};
use rpc::client::Rpc;
use rpc::RpcError;
use state_machine::{Config, RaftError};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;

use rand;
use rand::Rng;

///
/// # Usage
/// let raft_db = RaftConnection::new(Config::from_file("config"));
/// raft_db.command( ... );
/// let result = raft_db.query( ... );
///
pub struct RaftConnection {
    cluster: HashMap<u64, SocketAddr>, // Known peers
    leader_guess: SocketAddr,
}

impl RaftConnection {
    pub fn new(config: Config) -> Option<RaftConnection> {
        Some(RaftConnection { 
            cluster: config.cluster.clone(),
            leader_guess: config.cluster.iter().next().map(|(_, b)| *b).unwrap(),
        })
    }
    
    fn construct_client_request_rpc(buffer: &[u8], op: client_request::Op) -> Rpc {
        let mut rpc = Rpc::new(2); // TODO: make constants accessible
                   // Rpc::new(constants::CLIENT_REQUEST_OPCODE);
        {
            let mut params = rpc.get_param_builder()
                                .init_as::<client_request::Builder>();
            params.set_op(op);
            params.set_data(buffer.clone());
        }
        rpc
    }

    fn handle_client_reply(msg: Reader<OwnedSegments>) -> Result<Option<Vec<u8>>, RaftError> {
        Rpc::get_result_reader(&msg)
            .and_then(|result| {
                result.get_as::<client_request_reply::Reader>()
                      .map_err(RpcError::Capnp)
            })
            .map_err(RaftError::RpcError)
            .and_then(|reply| {
                // If we're not successful, set the leader guess if it exists!
                if !reply.get_success() {
                    let leader_guess = reply.get_leader_addr().ok()
                                            .map(|s|SocketAddr::from_str(s).unwrap());
                    Err(RaftError::NotLeader(leader_guess))
                } else {
                    let data = reply.get_data().ok()
                                    .map(|x| x.to_vec()); // map slice to vector
                    Ok(data)
                }
            })
    }

    fn choose_random_leader(&self) -> SocketAddr {
        let keys: Vec<u64> = self.cluster.keys().cloned().collect();
        let random_key = rand::thread_rng().choose(&keys).unwrap();
        self.cluster.get(random_key).unwrap().clone()
    }

    fn send_client_request(&mut self, buffer: &[u8], op: client_request::Op)
        -> Result<Option<Vec<u8>>, RaftError> {
        loop {
            let rpc = RaftConnection::construct_client_request_rpc(buffer, op);
            let result =  rpc.send(self.leader_guess)
                             .map_err(RaftError::RpcError)
                             .and_then(RaftConnection::handle_client_reply);
            // If "not leader," retry with new leader.
            if let Err(ref err) = result {
                if let RaftError::NotLeader(leader) = *err {
                    self.leader_guess = leader.unwrap_or(self.choose_random_leader());
                    continue;
                }
            }
            return result;
        }
    }

    pub fn command(&mut self, buffer: &[u8]) -> Result<(), RaftError> {
        self.send_client_request(buffer, client_request::Op::Write)
            .map(|_| {}) // Command to client should not return data...
    }

    pub fn query(&mut self, buffer: &[u8]) -> Result<Vec<u8>, RaftError> {
        self.send_client_request(buffer, client_request::Op::Read)
            .and_then(|option| {
                match option {
                    // If this succeeded, we should always get data back...
                    None => Err(RaftError::Unknown),
                    Some(data) => Ok(data)}})
    }
}

#[cfg(test)]
mod tests {
    use super::RaftConnection;
    use super::super::rpc::server::{RpcObject, RpcServer};
    use super::super::rpc::RpcError;
    use super::super::state_machine::Config;
    use super::super::raft_capnp::{client_request_reply};
    use capnp;
    use std::collections::HashMap;
    use std::net::{SocketAddr};
    use std::str::FromStr;
    use std::time::Duration;

    static LOCALHOST: &'static str = "127.0.0.1";

    struct ShittyClientRequestHandler {leader_port: u16}
    impl RpcObject for ShittyClientRequestHandler {
        fn handle_rpc (&self, params: capnp::any_pointer::Reader, result: capnp::any_pointer::Builder) 
            -> Result<(), RpcError>
            {
                let mut result_builder = result.init_as::<client_request_reply::Builder>();
                let leader_addr = format!("{}:{}", LOCALHOST, self.leader_port);
                result_builder.set_success(false);
                result_builder.set_leader_addr(&leader_addr);
                Ok(())
            }
    }

    struct LeaderClientRequestHandler {reply: Vec<u8>}
    impl RpcObject for LeaderClientRequestHandler {
    fn handle_rpc (&self, params: capnp::any_pointer::Reader, result: capnp::any_pointer::Builder) 
        -> Result<(), RpcError>
        {
            let mut result_builder = result.init_as::<client_request_reply::Builder>();
            result_builder.set_success(true);
            result_builder.set_data(&self.reply);
            Ok(())
        }
    }

    fn start_client_handler(port: u16, rpc_handler: Box<RpcObject>) -> RpcServer {
        let services = vec![(2, rpc_handler)];
        let mut server = RpcServer::new_with_services(services);
        let addr = format!("{}:{}", LOCALHOST, port);
        server.bind(&*addr).unwrap();
        server.repl().unwrap();
        server
    }

    fn start_shit_client_rpc_handler(port: u16, leader_port: u16) -> RpcServer {
        start_client_handler(port, Box::new(ShittyClientRequestHandler 
                                            { leader_port: leader_port }))
    }

    fn start_leader_client_rpc_handler(port: u16, reply: &[u8]) -> RpcServer {
        start_client_handler(port, Box::new(LeaderClientRequestHandler
                                            { reply: reply.to_vec() }))
    }

    fn get_dummy_config(cluster: HashMap<u64, SocketAddr>) -> Config {
        Config {
            cluster: cluster,
            // dummy "me" entry
            me: (1, SocketAddr::from_str("127.0.0.1:8005").unwrap()),
            heartbeat_timeout: Duration::from_millis(100),
        }
    }

    fn client_request_redirects_to_leader<F>(shit_port: u16, leader_port: u16, mut db_ops: F)
    where F: FnMut(RaftConnection) -> () {
        let shit_socket = format!("{}:{}", LOCALHOST, shit_port);
        let data = vec![];
        start_shit_client_rpc_handler(shit_port, leader_port);
        start_leader_client_rpc_handler(leader_port, &data);
        let mut cluster = HashMap::new();
        cluster.insert(0, SocketAddr::from_str(&*shit_socket).unwrap());
        db_ops(RaftConnection::new(get_dummy_config(cluster)).unwrap());
    }

    #[test]
    fn command_redirects_to_leader() {
        let leader_port = 8002;
        client_request_redirects_to_leader(
            8001, leader_port, |mut db| {
                let data = vec![];
                assert!(db.command(&data).is_ok());
                assert_eq!(db.leader_guess.to_string(),
                           format!("{}:{}", LOCALHOST, leader_port));
            });
    }

    #[test]
    fn query_redirects_to_leader() {
        let leader_port = 8003;
        client_request_redirects_to_leader(
            8004, leader_port, |mut db| {
                let data = vec![];
                assert!(db.query(&data).is_ok());
                assert_eq!(db.leader_guess.to_string(),
                           format!("{}:{}", LOCALHOST, leader_port));
            });
    }

    fn client_request_sends<F>(leader_port: u16, mut db_ops: F)
    where F: FnMut(RaftConnection) -> () {
        let leader_socket = format!("{}:{}", LOCALHOST, leader_port);
        let data = vec![];
        start_leader_client_rpc_handler(leader_port, &data);
        let mut cluster = HashMap::new();
        cluster.insert(0, SocketAddr::from_str(&*leader_socket).unwrap());
        db_ops(RaftConnection::new(get_dummy_config(cluster)).unwrap());
    }

    #[test]
    fn command_sends() {
        client_request_sends(8005, |mut db| {
            let data = vec![];
            assert!(db.command(&data).is_ok());
        });
    }

    #[test]
    fn query_sends() {
        client_request_sends(8006, |mut db| {
            let data = vec![];
            assert!(db.query(&data).is_ok());
        });
    }
}
