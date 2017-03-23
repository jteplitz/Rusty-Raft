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
use std::io::{stdin, SeekFrom, Error, ErrorKind, Read, BufReader, Seek, BufRead, Cursor, Write};

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
    let mut args = args();
    if let Some(command) = args.nth(1) {
        if command == String::from("server") {
            if let (Some(id_str), Some(filename)) =
                   (args.next(), args.next()) {
                if let Ok(id) = id_str.parse::<u64>() {
                    server(id, &cluster_from_file(filename));
                    return;
                }
            }
        } else if command == String::from("client") {
            if let Some(filename) = args.next() {
                client(&cluster_from_file(filename));
                return;
            }
        } else if command == String::from("servers") {
            if let Some(filename) = args.next() {
                servers(&cluster_from_file(filename));
                return;
            }
        }
    }
    println!("Incorrect usage. \n{}", USAGE);
}

fn servers(cluster: &HashMap<u64, SocketAddr>) {
    let servers : Vec<ServerHandle> =cluster.iter()
        .map(|(id, addr)| server(*id, &cluster.clone()))
        .collect();
    loop { }
}

fn server(id: u64, cluster: &HashMap<u64, SocketAddr>) -> ServerHandle {
    if !cluster.contains_key(&id) {
        panic!("Initial cluster must contain server id: {}", id);
    }
    start_server(id, cluster, Box::new(RaftHashMap { map: HashMap::new() })).unwrap()
}

// TODO (sydli): make this function less shit
fn process_command(hashmap: &mut ClientHashMap, words: Vec<String>) 
    -> bool {
    if words.len() == 0 { return false; }
    let ref cmd = words[0];
    if *cmd == String::from("get") {
        if words.len() <= 1 { return false; }
        words.get(1).map(|key| {
            match hashmap.get(key.clone()) {
                Ok(value) =>println!("Value for {} => {}", key, value),
                Err(err) =>println!("Error during get: {:?}", err),
            }
        }).unwrap();
    } else if *cmd == String::from("put") {
        if words.len() <= 2 { return false; }
        words.get(1).map(|key| { words.get(2).map(|val| {
            match hashmap.put(key.clone(), val.clone()) {
                Ok(()) => println!("Put {} => {}", key, val),
                Err(err) => println!("Error during put: {:?}", err),
            }
        }).unwrap()}).unwrap();
    } else { return false; }
    return true;
}

fn client(cluster: &HashMap<u64, SocketAddr>) {
    let mut hashmap = ClientHashMap::new(cluster);
    println!("Starting repl...");
    loop {
        let mut buffer = String::new();
        if stdin().read_line(&mut buffer).is_ok() {
                let words: Vec<String> = 
                    buffer.split_whitespace().map(String::from).collect();
                print!(" => ");
                if !process_command(&mut hashmap, words) {
                    println!(
                        "Invalid command! Valid commands are:\n\tget <key>\n\tput <key> <value>\n");
                }
        } else {
            println!("error processing");
        }
    }
}

fn io_err() -> std::io::Error {
    Error::new(ErrorKind::InvalidData, "File incorrectly formatted")
}

fn as_num(x: String) -> Result<u64, std::io::Error> {
    x.parse::<u64>().map_err(|_| io_err())
}

/// Panics on io error (we can't access the cluster info!)
/// TODO (sydli) make io_errs more informative
fn cluster_from_file(filename: String) -> HashMap<u64, SocketAddr> {
    let mut data = String::new();
    let file = File::open(filename.clone())
               .expect(&format!("Unable to open file {}", filename));
    let mut lines = BufReader::new(file).lines();
    lines.next().ok_or(io_err())
    .map(|line_or_io_error| line_or_io_error.unwrap())
    .and_then(as_num)
    .and_then(|num| (0..num).map(|_| {
        lines.next().ok_or(io_err())
        .map(|line_or_io_error| line_or_io_error.unwrap())
        .and_then(|node_str| {
            let mut words = node_str.split_whitespace().map(String::from);
            let id = words.next().ok_or(io_err())
                        .and_then(as_num);
            words.next().ok_or(io_err())
                .and_then(|addr_str| addr_str.parse::<SocketAddr>()
                                     .map_err(|_| io_err()))
                .and_then(move |addr| id.map(|id| (id, addr)))
        })
    }).collect::<Result<Vec<_>, _>>())
    .map(|nodes: Vec<(u64, SocketAddr)>|
        nodes.iter().cloned().collect::<HashMap<u64, SocketAddr>>()
    ).unwrap()
}

#[derive(RustcDecodable, RustcEncodable)]
struct Put {
    key: String,
    value: String,
}

pub struct ClientHashMap {
    raft: RaftConnection,
}

// TODO (sydli): Get rid of shitty json encode/decode...
impl ClientHashMap {
    fn new(cluster: &HashMap<u64, SocketAddr>) -> ClientHashMap {
        ClientHashMap { raft: RaftConnection::new_with_session(cluster).unwrap() }
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
        println!(" > > StateMachine: COMMAND");
        str::from_utf8(buffer)
            .map_err(deserialize_error)
            .and_then(|string| json::decode(&string)
                                    .map_err(deserialize_error))
            .map(|put: Put| 
                 { 
                     println!(" > > > PUT {} => {}", put.key, put.value);
                     self.map.insert(put.key, put.value);
                 })
    }

    fn query (& self, buffer: &[u8]) ->Result<Vec<u8>, RaftError> {
        println!(" > > StateMachine: QUERY");
        str::from_utf8(buffer)
            .map_err(deserialize_error)
            .and_then(|key| {
                let key = key.to_string();
                 println!(" > > > GET {}", key);
                self.map.get(&key)
                    .map(|x| x.as_bytes().to_vec())
                    .ok_or(key_error(&key))
            })
    }
}
