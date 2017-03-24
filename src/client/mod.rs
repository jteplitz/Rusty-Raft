pub mod state_machine;
use capnp::serialize::OwnedSegments;
use capnp::message::Reader;
use raft_capnp::{client_request};
use rpc::client::Rpc;
use common::{client_command, raft_query, raft_command, RaftError, SessionInfo};
use common::constants;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration};

use rand;
use rand::Rng;

/// Base amount of time to backoff when a client request fails.
const BACKOFF_TIME_MS:u64 = 50;
const MIN_RETRY:u64 = 3;

///
/// # Usage
/// ```rust,no_run
/// # use rusty_raft::client::RaftConnection;
/// # use std::collections::HashMap;
/// let cluster = HashMap::new(); // This map should be actually indicative of
///                               // your cluster configuration.
///                               // (it maps server id => server sockets)
/// let mut raft_db = RaftConnection::new_with_session(&cluster).unwrap();
/// // Command and data buffers should be serialized/deserialized by client.
/// // These will be passed on to the state machines running on your cluster.
/// raft_db.command(&vec![1, 2, 3, 4]);
/// let result = raft_db.query(&vec![1, 2, 3, 4]);
/// ```
///
/// These will always succeed because RaftConnection will continue retrying
/// until the request terminates (with an increasing backoff timeout). They are
/// implicitly guaranteed to terminate due to the properties of the Raft protocol.
/// Optionally, you can call |command_timeout| or |query_timeout| to guarantee
/// termination.
///
/// Note: not thread-safe. Wrap object in Mutex if shared across threads.
///
#[derive(Debug)]
pub struct RaftConnection {
    cluster: HashMap<u64, SocketAddr>,  // Known peers
    pub leader_guess: SocketAddr,           // Current guess for leader. TODO (sydli) unpub this
    // after testing
    backoff_time: Duration,             // Base backoff time.
    client_id: Option<u64>,             // client id for current session
    sequence_number: u64,               // client id for current session
}

impl RaftConnection {
    ///
    /// New RaftConnection for testing.
    ///
    fn new(cluster: &HashMap<u64, SocketAddr>, client_id: Option<u64>) -> RaftConnection {
        RaftConnection { 
            cluster: cluster.clone(),
            leader_guess: cluster.iter().next().map(|(_, b)| *b).unwrap(),
            backoff_time: Duration::from_millis(BACKOFF_TIME_MS),
            client_id: client_id,
            sequence_number: 0,
        }
    }

    fn get_session(&mut self) -> SessionInfo {
        if self.client_id.is_none() {
            self.register_client().unwrap();
        }
        self.sequence_number += 1;
        SessionInfo {
            client_id: self.client_id.unwrap(),
            sequence_number: self.sequence_number,
        }
    }

    ///
    /// Creates a mock (sessionless) RaftConnection using the initial cluster configuration.
    ///
    #[cfg(test)]
    fn new_mock(cluster: &HashMap<u64, SocketAddr>) -> RaftConnection {
        RaftConnection::new(cluster, Some(0))
    }

    fn handle_register_client_reply(msg: Reader<OwnedSegments>) -> Result<(), RaftError> {
        Rpc::get_result_reader(&msg)
            .map_err(|x| RaftError::ClientError(format!("{:?}", x)))// jank
            .and_then(|result| {
                client_command::reply_from_proto(
                &mut result.get_as::<client_request::reply::Reader>().unwrap())
            })
            .and_then(|result| {
                if let client_command::Reply::Command(reply) = result {
                  if raft_command::Reply::OpenSession == reply {
                    return Ok(())
                  }
                }
                Err(RaftError::Unknown)
            })
    }

    ///
    /// Registers this client with the current leader.
    ///
    fn register_client(&mut self) -> Result<(), RaftError> {

        let session_id = rand::thread_rng().next_u64();
        self.perform_leader_op(|leader_addr|  {
            let mut rpc = Rpc::new(constants::CLIENT_REQUEST_OPCODE);
            {
                let mut params = rpc.get_param_builder()
                                    .init_as::<client_request::Builder>();
                client_command::request_to_proto(
                    client_command::Request::Command(
                        raft_command::Request::OpenSession(session_id)),
                    &mut params);
            }
            rpc.send(leader_addr)
               .map_err(|x| RaftError::IoError(format!("{:?}", x)))// jank
               .and_then(RaftConnection::handle_register_client_reply)
        }).map(|x| {
            self.client_id = Some(session_id);
            x
        })
    }

    ///
    /// Opens a new session with the Raft cluster specified.
    ///
    pub fn new_with_session(cluster: &HashMap<u64, SocketAddr>)
        -> Option<RaftConnection> {
        let mut conn = RaftConnection::new(cluster, None);
        conn.register_client().ok().map(|_| conn)
    }

    ///
    /// Helper to construct a ClientRequest rpc from |buffer| (the data to pass
    /// to the request) and |op| (the type of the request).
    ///
    fn construct_client_request_rpc(op: client_command::Request) -> Rpc {
        let mut rpc = Rpc::new(constants::CLIENT_REQUEST_OPCODE);
        {
            let mut params = rpc.get_param_builder()
                                .init_as::<client_request::Builder>();
            client_command::request_to_proto(op, &mut params);
        }
        rpc
    }

    ///
    /// Helper to transform rpc response
    /// into Option<Vec<u8>> if the client succeeded, otherwise into a
    /// NotLeader error. If reply contained a leader guess, use that; otherwise
    /// just choose another random leader.
    ///
    /// |msg| must contain a client_request_reply::Reader.
    ///
    fn handle_client_reply(msg: Reader<OwnedSegments>) -> Result<client_command::Reply, RaftError> {
        Rpc::get_result_reader(&msg)
            .map_err(|x| RaftError::ClientError(format!("{:?}", x)))// jank
            .and_then(|result| {
                client_command::reply_from_proto(
                    &mut result.get_as::<client_request::reply::Reader>().unwrap())
            })
    }

    ///
    /// Helper to retrieve a random leader from our initial cluster
    ///
    fn choose_random_leader(&self) -> u64 {
        let keys: Vec<u64> = self.cluster.keys().cloned().collect();
        *rand::thread_rng().choose(&keys).unwrap()
    }

    ///
    /// Performs |leader_op| on guessed leader address. If |leader_op| results in
    /// a |NotLeader| error, try again with either a random new leader or
    /// the leader hint in the error reply. Otherwise return the result of |leader_op|.
    ///
    fn perform_leader_op<T, F>(&mut self, leader_op: F) -> Result<T, RaftError>
        where F: Fn(SocketAddr) -> Result<T, RaftError> {
        let mut backoff_multiplier = 0;
        let mut num_retries = (MIN_RETRY + self.cluster.len() as u64) as i32;
        while num_retries > 0 {
            let result = leader_op(self.leader_guess);
            // If "not leader," retry with new leader.
            if let Err(ref err) = result {
                match *err {
                    RaftError::NotLeader(leader) => {
                        self.leader_guess = self.cluster.get(&leader.unwrap_or(self.choose_random_leader())).unwrap().clone();
                        backoff_multiplier += 1;
                        num_retries -= 1;
                        thread::sleep(self.backoff_time * backoff_multiplier);
                        continue;
                    },
                    RaftError::IoError(_) => { // Maybe the leader is down
                        self.leader_guess = self.cluster.get(&self.choose_random_leader()).unwrap().clone();
                        num_retries -= 1;
                        backoff_multiplier += 1;
                        thread::sleep(self.backoff_time * backoff_multiplier);
                        continue;
                    },
                    _ => (),
                }
            }
            return result;
        }
        return Err(RaftError::IoError(String::from("Couldn't reach leader in cluster.")));
    }

    ///
    /// Sends a client request to the leader of a cluster.
    ///
    fn send_client_request(&mut self, op: client_command::Request)
        -> Result<client_command::Reply, RaftError> {
        self.perform_leader_op(move |leader_addr|  {
            RaftConnection::construct_client_request_rpc(op.clone())
                .send(leader_addr)
                .map_err(|x| RaftError::IoError(format!("{:?}", x)))// jank
                .and_then(RaftConnection::handle_client_reply)
        })
    }

    ///
    /// Sends this Raft cluster a command with data |buffer|.
    ///
    /// #Errors
    /// RaftError if Rpc or Client's state machine fails.
    ///
    pub fn command(&mut self, buffer: &[u8]) -> Result<(), RaftError> {
        let session = self.get_session();
        self.send_client_request(client_command::Request::Command(
                raft_command::Request::StateMachineCommand {
                data: buffer.to_vec(),
                session: session}))
            .map(|_| {}) // Command to client should not return data...
    }

    ///
    /// Sends this Raft cluster a query with data |buffer|, and returns
    /// the queried data buffer from the state machine on success.
    ///
    /// #Errors
    /// RaftError if Rpc or Client's state machine fails.
    ///
    pub fn query(&mut self, buffer: &[u8]) -> Result<Vec<u8>, RaftError> {
        self.send_client_request(client_command::Request::Query(
                raft_query::Request::StateMachineQuery(buffer.to_vec())))
            .and_then(|reply| {
                if let client_command::Reply::Query(reply) = reply {
                    match reply {
                        raft_query::Reply::StateMachineQuery(data) => {
                            return Ok(data);
                        }
                    }
                }
                Err(RaftError::Unknown)
            })
    }

    pub fn add_server(&mut self, id: u64, addr: SocketAddr) -> Result<(), RaftError> {
        self.send_client_request(client_command::Request::AddServer((id, addr)))
        // sucessful AddServer RPCs don't return anything
        .map(|_| {})?;
        // add this server to our cached cluster map
        self.cluster.insert(id, addr);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{RaftConnection, BACKOFF_TIME_MS};
    use super::super::rpc::server::{RpcObject, RpcServer};
    use super::super::rpc::RpcError;
    use super::super::common::{client_command, RaftError };
    use super::super::raft_capnp::{client_request as proto};
    use capnp;
    use std::collections::HashMap;
    use std::net::{SocketAddr};
    use std::str::FromStr;
    use std::time::{Duration, Instant};

    static LOCALHOST: &'static str = "127.0.0.1";

    ///
    /// ClientRequest handler that simply fails
    /// and redirects the client to the "leader" at |leader_id|.
    ///
    struct RedirectClientRequestHandler {leader_id: u64}
    impl RpcObject for RedirectClientRequestHandler {
        fn handle_rpc (&self, _: capnp::any_pointer::Reader, result: capnp::any_pointer::Builder) 
            -> Result<(), RpcError>
            {
                let mut result_builder = result.init_as::<proto::reply::Builder>();
                let err = RaftError::NotLeader(Some(self.leader_id));
                client_command::reply_to_proto(Err(err), &mut result_builder);
                Ok(())
            }
    }

    ///
    /// ClientRequest handler that always responds successfully
    /// with the data in |reply|.
    ///
    struct LeaderClientRequestHandler {}
    impl RpcObject for LeaderClientRequestHandler {
    fn handle_rpc (&self, params: capnp::any_pointer::Reader, result: capnp::any_pointer::Builder) 
        -> Result<(), RpcError>
        {
            let op = client_command::request_from_proto(
               params.get_as::<proto::Reader>().unwrap());
            let reply = client_command::successful_reply_for(op);
            let mut result_builder = result.init_as::<proto::reply::Builder>();
            client_command::reply_to_proto(Ok(reply), &mut result_builder);
            Ok(())
        }
    }

    ///
    /// |rpc_handler| must be a Box< a ClientRequestHandler >.
    /// Starts Rpc server for |rpc_handler| and returns the port it
    /// was attached to.
    ///
    fn start_client_handler(rpc_handler: Box<RpcObject>) -> (u16, RpcServer) {
        let services = vec![(2, rpc_handler)];
        let mut server = RpcServer::new_with_services(services);
        let mut port = 8000;
        loop {
            let addr = format!("{}:{}", LOCALHOST, port);
            if server.bind(&*addr).is_ok() { break; }
            port += 1;
        }
        server.repl().unwrap();
        (port, server)
    }

    fn start_redirect_client_rpc_handler(redirect_id: u64) -> (u16, RpcServer) {
        start_client_handler(Box::new(RedirectClientRequestHandler 
                                      { leader_id: redirect_id}))
    }

    fn start_leader_client_rpc_handler() -> (u16, RpcServer) {
        start_client_handler(Box::new(LeaderClientRequestHandler
                                      {}))
    }

    ///
    /// Helper function for testing! Creates servers that "redirect" to each
    /// other in a chain:
    ///   C_1 -> C_2 -> C_3 ... -> C_N -> LEADER
    /// where N = |chain_size|. Only the LEADER server replies to the client
    /// request successfully; any other machine C_X will reply with a failure
    /// and a "leader_guess" for C_X+1. The |cluster| in Config contains
    /// only C_1 (or LEADER if |chain_size| == 0), forcing all requests to
    /// start at the beginning of the chain.
    ///
    /// |chain_size| is >= 0. If chain_size is 0, there's a single
    /// LEADER in the cluster so operations should terminate immediately.
    /// 
    /// |db_op| contains the operation to perform on the resulting 
    /// RaftConnection object, and should contain at least one |command|
    /// or |query| call.
    ///
    /// This function also asserts that |db_op| is computed within
    /// the expected backoff time of the chain.
    ///
    fn client_request_redirects_to_leader<F>(chain_size: u64, db_op: F)
    where F: Fn(&mut RaftConnection) -> () {
        // Create leader ...
        let (leader_port, _server) = start_leader_client_rpc_handler();
        let leader_socket = format!("{}:{}", LOCALHOST, leader_port);
        let mut cluster = HashMap::new();
        let mut backoff_bound = (0, BACKOFF_TIME_MS);
        let mut redirect_servers = Vec::new();
        cluster.insert(0, SocketAddr::from_str(&*leader_socket).unwrap());
        // Construct redirect chain of clients ...
        for i in 1 .. chain_size + 1 {
            let (redirect_port, server) = start_redirect_client_rpc_handler(i - 1);
            redirect_servers.push(server);
            let redirect_socket = format!("{}:{}", LOCALHOST, redirect_port);
            cluster.insert(i, SocketAddr::from_str(&*redirect_socket).unwrap());
            backoff_bound = (backoff_bound.0 + (i) * BACKOFF_TIME_MS,
                             backoff_bound.1 + (i + 1) * BACKOFF_TIME_MS);
        }
        // Create and operate on RaftConnection ...
        let start_time = Instant::now();
        let mut db = RaftConnection::new_mock(&cluster);
        db_op(&mut db);
        // Make sure the db_op computed within the expected backoff time
        // of the chain.
        let command_duration = Instant::now().duration_since(start_time);
        assert!(command_duration > Duration::from_millis(backoff_bound.0));
        // assert!(command_duration < Duration::from_millis(backoff_bound.1));
        assert_eq!(db.leader_guess.to_string(),
                   format!("{}:{}", LOCALHOST, leader_port));
    }

    #[test]
    fn command_redirects_to_leader() {
        let data = vec![];
        client_request_redirects_to_leader(3,
            |db| { assert!((*db).command(&data).is_ok()); });
    }

    #[test]
    fn query_redirects_to_leader() {
        let data = vec![];
        client_request_redirects_to_leader(3, 
            |db| { assert!((*db).query(&data).is_ok()); });
    }

    #[test]
    fn open_session_redirects_to_leader() {
        client_request_redirects_to_leader(3, 
            |db| { assert!((*db).register_client().is_ok()); });
    }

    #[test]
    fn command_sends() {
        let data = vec![];
        client_request_redirects_to_leader(0,
            |db| { assert!((*db).command(&data).is_ok()); });
    }

    #[test]
    fn query_sends() {
        let data = vec![];
        client_request_redirects_to_leader(0,
            |db| { assert!((*db).query(&data).is_ok()); });
    }

    #[test]
    fn open_session_sends() {
        client_request_redirects_to_leader(0, 
            |db| { assert!((*db).register_client().is_ok()); });
    }
}
