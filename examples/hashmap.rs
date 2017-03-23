#[macro_use] extern crate log;
extern crate rusty_raft;
extern crate rand;
extern crate rustc_serialize;

use rand::{thread_rng, Rng};
use rusty_raft::server::{start_server, ServerHandle};
use rusty_raft::client::{RaftConnection};
use rusty_raft::client::state_machine::{
    StateMachine, RaftStateMachine};
use rusty_raft::common::{Config, RaftError};
use std::env::args;
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::str;
use std::str::FromStr;
use std::time::Duration;

use std::fs::File;
use std::io::{stdin, stdout, SeekFrom, Error, ErrorKind, Read, BufReader, Seek, BufRead, Cursor, Write};

use rustc_serialize::json;

static USAGE: &'static str = "
Commands:
  server  Starts a server from an initial cluster config.
  client  Starts a client repl, that communicates with
          a particular cluster.

Usage:
  hashmap server <id> <filename>
  hashmap servers <filename>
  hashmap client <filename>

Options:
  -h --help   Show a help message.
";

fn main() {
    // TODO (sydli) make prettier
    let mut args = args();
    if let Some(command) = args.nth(1) {
        if &command == "server" {
            if let (Some(id_str), Some(filename)) =
                   (args.next(), args.next()) {
                if let Ok(id) = id_str.parse::<u64>() {
                    Server::new(id, &cluster_from_file(&filename)).repl();
                    return;
                }
            }
        } else if &command == "client" {
            if let Some(filename) = args.next() {
                Client::new(&cluster_from_file(&filename)).repl();
                return;
            }
        } else if &command == "servers" {
            if let Some(filename) = args.next() {
                Cluster::new(&cluster_from_file(&filename)).repl();
                return;
            }
        }
    }
    warn!("Incorrect usage. \n{}", USAGE);
}

///
/// Automatically builds a repl loop for an object, if it implements |exec|.
/// Each line a user types into the repl is fed to |exec|.
///
/// Default commands:
///     exit    Exits the loop and shuts down associated process.
///     help    Displays the |help| message for more usage info.
trait Repl {

    ///
    /// Processes each input command from the Repl.
    /// Returns true if |command| is formatted correctly.
    ///
    fn exec(&mut self, command: String) -> bool;

    ///
    /// Usage string displayed with the default command "help".
    ///
    fn usage(&self) -> String;

    fn print_usage(&self) {
        println!("\nREPL COMMANDS:\n==============\n{}\n{}\n{}",
                 "exit\n\t\tExits the loop and shuts down associated process.",
                 "help\n\t\tSpits out this message.",
                 self.usage());
    }

    ///
    /// Implementation of the repl.
    fn repl(&mut self) {
        println!("[ Starting repl ]");
        loop {
            print!("> ");
            stdout().flush().unwrap();
            let mut buffer = String::new();
            if stdin().read_line(&mut buffer).is_ok() {
                let words: Vec<String> = 
                    buffer.split_whitespace().map(String::from).collect();
                if words.get(0) == Some(&String::from("exit")) { break; }
                if words.get(0) == Some(&String::from("help")) { 
                    self.print_usage();
                    continue;
                }
                if !self.exec(buffer.clone()) {
                    println!("Command not recognized.");
                    self.print_usage();
                }
            }
        }
    }
}

struct Server {
    handle: ServerHandle,
}

impl Repl for Server {
    fn exec(&mut self, command: String) -> bool { true }
    fn usage(&self) -> String { String::from("") }
}

impl Server {
    fn new(id: u64, cluster: &HashMap<u64, SocketAddr>) -> Server {
        if !cluster.contains_key(&id) {
            panic!("Initial cluster must contain server id: {}", id);
        }
        Server { 
            handle: 
                // TODO: Only the first server should put itself in its log file
                start_server(id, Box::new(RaftHashMap { map: HashMap::new() }), *cluster.get(&id).unwrap()).unwrap()
        }
    }
}


struct Cluster {
    servers: HashMap<u64, Server>,
    cluster: HashMap<u64, SocketAddr>,
}

impl Cluster {
    fn new(cluster: &HashMap<u64, SocketAddr>) -> Cluster {
        Cluster {
            servers: cluster.iter()
                .map(|(id, _)| (*id, Server::new(*id, &cluster.clone())))
                .collect::<HashMap<u64, Server>>(),
            cluster: cluster.clone(),
        }
    }

    fn add_server(&mut self, id: u64, addr: SocketAddr) {
        println!("Adding server {}, {}", id, addr);
    }

    fn remove_server(&mut self, id: u64) {
        if !self.cluster.contains_key(&id) {
            println!("Server {} is not in the cluster! Servers: {:?}", id,
                     self.cluster);
        }
        println!("Removing server {}", id);
    }

    fn kill_server(&mut self, id: u64) {
        if !self.servers.contains_key(&id) {
            println!("Server {} is not up right now!", id);
        }
        {
            self.servers.remove(&id).unwrap();
        } // drop server
        println!("Killed server {}", id);
    }

    fn start_server(&mut self, id: u64) {
        if self.servers.contains_key(&id) {
            println!("Server {} is already up!", id);
        }
        self.servers.insert(id, Server::new(id, &self.cluster.clone()));
        println!("Restarted server {}", id);
    }

    fn print_servers(&self) {
        println!("Servers in cluster: {:?}\n Live servers: {:?}",
                 self.cluster, self.servers.keys().collect::<Vec<&u64>>());
    }
}

impl Repl for Cluster {
    fn exec(&mut self, command: String) -> bool {
        let words: Vec<&str> = 
            command.split_whitespace().collect();
        let first = words.get(0).map(|x|*x);
        if first == Some("add")  {
            let num = words.get(1).and_then(|x| as_num(*x).ok());
            let addr = words.get(2).and_then(|x| as_addr(*x).ok());
            if num.and(addr).is_none() { return false; }
            self.add_server(num.unwrap(), addr.unwrap());
        } else if first == Some("remove") {
            let num = words.get(1).and_then(|x| as_num(*x).ok());
            if num.is_none() { return false; }
            self.remove_server(num.unwrap());
        } else if first == Some("kill") {
            let num = words.get(1).and_then(|x| as_num(*x).ok());
            if num.is_none() { return false; }
            self.kill_server(num.unwrap());
        } else if first == Some("start") {
            let num = words.get(1).and_then(|x| as_num(*x).ok());
            if num.is_none() { return false; }
            self.start_server(num.unwrap());
        } else if first == Some("list") {
            self.print_servers();
        } else { return false; }
        return true;
    }

    fn usage(&self) -> String { 
        String::from(format!(
                "{}\n{}\n{}\n{}\n{}",
                "add <node-id> <node-addr>\tAdds a server to the cluster.",
                "remove <node-id>\tRemoves a server from the cluster.",
                "start <node-id>\tStarts a server from the cluster.",
                "kill <node-id>\tKills a server from the cluster.",
                "list\tLists all cluster servers, and the ones that are live."))
    }
}


struct Client {
    raft: RaftConnection,
}

impl Repl for Client {
    fn exec (&mut self, command: String) -> bool {
        let words: Vec<String> = 
            command.split_whitespace().map(String::from).collect();
        self.process_command(words)
    }
    fn usage(&self) -> String { 
        String::from(format!(
                "{}\n{}",
                 "get <key>\n\t\tPrints value of <key> in hashmap, if it exists.",
                 "put <key> <value>\n\t\tPuts value of <key> as <value>"))
    }
}

#[derive(RustcDecodable, RustcEncodable)]
struct Put {
    key: String,
    value: String,
}

impl Client {
    fn new(cluster: &HashMap<u64, SocketAddr>) -> Client {
        let connection = RaftConnection::new_with_session(cluster);
        if connection.is_none() {
            println!("Couldn't establish connection to cluster at {:?}", cluster);
            panic!();
        }
        Client { raft: RaftConnection::new_with_session(cluster).unwrap() }
    }

    fn get(&mut self, key: String) -> Result<String, RaftError> {
        self.raft.query(key.as_bytes())
            .and_then(
                |result| str::from_utf8(&result)
                             .map(str::to_string)
                             .map_err(deserialize_error))
    }

    fn put(&mut self, key: String, value: String) -> Result<(), RaftError> {
        json::encode(&Put {key:key, value:value})
            .map_err(serialize_error)
            .and_then(|buffer| self.raft.command(&buffer.as_bytes()))
    }

    // TODO (sydli): make this function less shit
    fn process_command(&mut self, words: Vec<String>) 
        -> bool {
        if words.len() == 0 { return true; }
        let ref cmd = words[0];
        if *cmd == String::from("get") {
            if words.len() <= 1 { return false; }
            words.get(1).map(|key| {
                match self.get(key.clone()) {
                    Ok(value) => println!("Value for {} is {}", key, value),
                    Err(err) => println!("Error during get: {:?}", err),
                }
            }).unwrap();
        } else if *cmd == String::from("put") {
            if words.len() <= 2 { return false; }
            words.get(1).map(|key| { words.get(2).map(|val| {
                match self.put(key.clone(), val.clone()) {
                    Ok(()) => println!("Put {} => {} successfully", key, val),
                    Err(err) => println!("Error during put: {:?}", err),
                }
            }).unwrap()}).unwrap();
        } else { return false; }
        return true;
    }
}

fn io_err() -> std::io::Error {
    Error::new(ErrorKind::InvalidData, "File incorrectly formatted")
}

fn as_num(x: &str) -> Result<u64, std::io::Error> {
    String::from(x).parse::<u64>().map_err(|_| io_err())
}

fn as_addr(x: &str) -> Result<SocketAddr, std::io::Error> {
    String::from(x).parse::<SocketAddr>().map_err(|_| io_err())
}

/// Panics on io error (we can't access the cluster info!)
/// TODO (sydli) make io_errs more informative
// TODO(jason): Remove this and bootstrap a dynamic cluster
fn cluster_from_file(filename: &str) -> HashMap<u64, SocketAddr> {
    let file = File::open(filename.clone())
               .expect(&format!("Unable to open file {}", filename));
    let mut lines = BufReader::new(file).lines();
    lines.next().ok_or(io_err())
    .map(|line_or_io_error| line_or_io_error.unwrap())
    .and_then(|x| as_num(&x))
    .and_then(|num| (0..num).map(|_| {
        lines.next().ok_or(io_err())
        .map(|line_or_io_error| line_or_io_error.unwrap())
        .and_then(|node_str| {
            let mut words = node_str.split_whitespace().map(String::from);
            let id = words.next().ok_or(io_err())
                        .and_then(|x| as_num(&x));
            words.next().ok_or(io_err())
                .and_then(|x| as_addr(&x))
                .and_then(move |addr| id.map(|id| (id, addr)))
        })
    }).collect::<Result<Vec<_>, _>>())
    .map(|nodes: Vec<(u64, SocketAddr)>|
        nodes.iter().cloned().collect::<HashMap<u64, SocketAddr>>()
    ).unwrap()
}

struct RaftHashMap {
    map: HashMap<String, String>,
}

fn serialize_error <T: Debug>(error: T) -> RaftError {
        RaftError::ClientError(
            format!("Couldn't serialize object. Error: {:?}", error))
}

fn deserialize_error <T: Debug>(error: T) -> RaftError {
        RaftError::ClientError(
            format!("Couldn't deserialize buffer. Error: {:?}", error))
}

fn key_error(key: &String) -> RaftError {
        RaftError::ClientError(format!("Couldn't find key {}", key))
}

impl StateMachine for RaftHashMap {
    fn command (&mut self, buffer: &[u8]) ->Result<(), RaftError> {
        str::from_utf8(buffer)
            .map_err(deserialize_error)
            .and_then(|string| json::decode(&string)
                                    .map_err(deserialize_error))
            .map(|put: Put| 
                 { 
                     self.map.insert(put.key, put.value);
                 })
    }

    fn query (& self, buffer: &[u8]) ->Result<Vec<u8>, RaftError> {
        str::from_utf8(buffer)
            .map_err(deserialize_error)
            .and_then(|key| {
                let key = key.to_string();
                self.map.get(&key)
                    .map(|x| x.as_bytes().to_vec())
                    .ok_or(key_error(&key))
            })
    }
}

