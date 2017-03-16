//! A TCP RelayServer that intercepts messages between servers inside an
//! integration test. Right now it just allows us to simulate nodes going offline
//! but we could extend it to expose information about which messages have been sent
//! for testing asserts.
//!
//! #Examples
//!
//! ```
//! let relay_server = RelayServer::new_with_random_addresses(NUM_ADDRESSES);
//! let addrs = relay_server.get_bound_addresses();
//! for addr in addrs {
//!     // start up a new Raft server (can pass in all bound addrs as the addrs for the other
//!     // servers)
//!     # let raft_server_addr = "127.0.0.1:0";
//!     relay_server.relay_address(addr, raft_server_addr);
//! }
//! // relay server is now set up and forwarding messages between raft servers.
//! ```
//!


use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream, Shutdown};
use std::thread;
use std::sync::{Arc, Mutex};
use std::io::{Read, Write, Error};

macro_rules! println_stderr(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

pub struct RelayServer {
    addrs: Arc<Mutex<HashMap<SocketAddr, SocketInfo>>>
}

#[derive(Clone)]
struct SocketInfo {
    to_addr: Option<SocketAddr>,
    online: bool
}

impl RelayServer {
    pub fn new() -> RelayServer {
        RelayServer {addrs: Arc::new(Mutex::new(HashMap::new()))}
    }

    pub fn new_with_random_addresses(num_addresses: u64) -> RelayServer {
        let mut server = RelayServer::new();
        server.bind_random_addresses(num_addresses);
        server
    }

    /// Binds to num_addresses addresses. Ports are assigned by the OS.
    /// Does not initially route their messages anywhere
    /// 
    /// #Panics
    /// Panics if it is unable to acquire enough bound TCP ports.
    /// Or if one of the server's background threads have panicked.
    pub fn bind_random_addresses(&mut self, num_addresses: u64) {
        let mut addrs = self.addrs.lock().unwrap();
        for _ in 0..num_addresses {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let socket_info = SocketInfo {to_addr: None, online: false};
            addrs.insert(listener.local_addr().unwrap(), socket_info);

            let addrs_clone = self.addrs.clone();
            // spawn a background thread to listen on this port
            thread::spawn (move || RelayServer::relay_messages(listener, addrs_clone));
        }
    }

    /// Returns a vector containing the bound socket addrs
    pub fn get_bound_addresses(&self) -> Vec<SocketAddr> {
        let addrs = self.addrs.lock().unwrap();
        addrs.keys()
             .cloned()
             .take(addrs.len())
             .collect::<Vec<SocketAddr>>()
    }

    /// Sets up a new mapping from from_address to to_address.
    /// Initially the mapping is online. Use set_address_active to change this
    ///
    /// #Panics
    /// Panics if the from_address is not an existing mapping
    /// Also panics if any of the server's background threads have panicked.
    pub fn relay_address(&mut self, from_address: SocketAddr, to_address: SocketAddr) {
        let mut addrs = self.addrs.lock().unwrap();
        let socket_info: &mut SocketInfo = addrs.get_mut(&from_address).unwrap();

        socket_info.to_addr = Some(to_address);
        socket_info.online = true;
    }

    /// Toggles a mapping online or offline.
    ///
    /// #Panics
    /// Panics if the from_address is not an existing mapping
    /// Also panics if any of the server's background threads have panicked.
    pub fn set_address_active(&mut self, from_address: SocketAddr, online: bool) {
        let mut addrs = self.addrs.lock().unwrap();
        addrs.get_mut(&from_address).unwrap().online = online;
    }

    /// Runs in a background thread, and relays messages according to the hash map.
    fn relay_messages(listener: TcpListener, addrs: Arc<Mutex<HashMap<SocketAddr, SocketInfo>>>) {
        let local_addr = listener.local_addr().unwrap();
        let mut counter = 0;
        for stream in listener.incoming() {
            match stream {
                Ok (stream) => {
                    counter += 1;
                    let addr_info = { addrs.lock().unwrap().get(&local_addr).cloned() };
                    match addr_info {
                        Some(info) => {
                            if info.online {
                                match RelayServer::relay_stream_to_addr(stream, info.to_addr.unwrap()) {
                                    Ok(_) => {},
                                    Err(e) => {
                                        println_stderr!("IO Error when connecting to Raft Server! {}", e);
                                    }
                                }
                            }
                        },
                        None => {}
                    }
                },
                Err (_) => {
                    // currently none of our tests should cause this kind of
                    // connection error
                    panic!("Connection error");
                }
            }
        }
    }

    /// Relays the given stream to the to_addr and sends the result back to the stream.
    ///
    /// #Panics
    /// Panics if the OS does not allow cloning the tcp streams
    fn relay_stream_to_addr(stream: TcpStream, to_addr: SocketAddr) -> Result<(), Error> {
        // Open a connection to to_addr
        let outgoing = try!(TcpStream::connect(to_addr));

        let outgoing_clone = outgoing.try_clone().unwrap();
        let stream_clone = stream.try_clone().unwrap();
        thread::spawn (move || RelayServer::relay_stream_to_stream(outgoing_clone, stream_clone));

        thread::spawn (move || RelayServer::relay_stream_to_stream(stream, outgoing));

        Ok(())
    }

    /// Relays messages from from_stream to to_stream until
    /// EOF is reached on from_stream. At this point we send
    /// an EOF on to_stream
    ///
    /// #Panics 
    /// Panics if the from_stream cannot be read from or the to_stream can
    /// not be written to
    fn relay_stream_to_stream(mut from_stream: TcpStream, mut to_stream: TcpStream) {
        let mut buf = [0; 100];
        loop {
            let bytes_read = from_stream.read(&mut buf[..]).unwrap();
            if bytes_read == 0 {
                to_stream.shutdown(Shutdown::Write).unwrap();
                break;
            }
            to_stream.write(&buf[0..bytes_read]).unwrap();
            to_stream.flush().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;
    use super::*;
    use std::thread;
    use std::sync::mpsc::{channel, Receiver};
    use std::net::{SocketAddr, TcpListener, TcpStream, Shutdown};
    use std::io::{Read, Write};
    use self::rand::{thread_rng, Rng};
    use std::time::Duration;

    // Timeout value, in ms, when waiting for local network messages
    const TIMEOUT: u64 = 5000;

    #[test]
    /// Tests that the relay server will relay from A->B
    fn it_relays_from_client() {
        const MESSAGE_LENGTH: usize = 1057;

        let mut relay_server = RelayServer::new_with_random_addresses(1);
        let addresses = relay_server.get_bound_addresses();
        assert_eq!(addresses.len(), 1);

        let (addr, rx) = start_tcp_listening_server();
        relay_server.relay_address(addresses[0], addr);

        let msg: String = thread_rng().gen_ascii_chars().take(MESSAGE_LENGTH).collect();
        {
            let mut client = TcpStream::connect(addresses[0]).unwrap();
            // generate a random message string
            assert_eq!(msg.as_bytes().len(), MESSAGE_LENGTH);
            client.write_all(msg.as_bytes()).unwrap();
            client.flush().unwrap();
        }
        
        let result = rx.recv_timeout(Duration::from_millis(TIMEOUT)).unwrap();
        assert_eq!(msg.as_bytes(), &result[..]);
    }

    #[test]
    fn it_relays_from_server() {
        const MESSAGE_LENGTH: usize = 904;

        let mut relay_server = RelayServer::new_with_random_addresses(1);
        let addresses = relay_server.get_bound_addresses();
        assert_eq!(addresses.len(), 1);

        let msg: String = thread_rng().gen_ascii_chars().take(MESSAGE_LENGTH).collect();
        let addr = start_tcp_sending_server(msg.clone());
        relay_server.relay_address(addresses[0], addr);

        let mut client = TcpStream::connect(addresses[0]).unwrap();
        let mut v = Vec::new();
        client.read_to_end(&mut v).unwrap();
        assert_eq!(&v[..], msg.as_bytes());
    }

    #[test]
    fn it_relays_from_client_and_server() {
        const MESSAGE_LENGTH: usize = 904;

        let mut relay_server = RelayServer::new_with_random_addresses(1);
        let addresses = relay_server.get_bound_addresses();
        assert_eq!(addresses.len(), 1);

        let msg: String = thread_rng().gen_ascii_chars().take(MESSAGE_LENGTH).collect();
        let addr = start_tcp_echo_server();
        relay_server.relay_address(addresses[0], addr);

        let mut client = TcpStream::connect(addresses[0]).unwrap();
        client.write_all(msg.as_bytes()).unwrap();
        client.flush().unwrap();
        client.shutdown(Shutdown::Write).unwrap();

        let mut v = Vec::new();
        client.read_to_end(&mut v).unwrap();

        assert_eq!(&v[..], msg.as_bytes());
    }

    /// Starts a simple TCP server on the returned address (OS assigned) in a background thread
    /// that sends every message it recieves down through the returned channel
    fn start_tcp_listening_server() -> (SocketAddr, Receiver<Vec<u8>>) {
        let (tx, rx) = channel();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let mut s = stream.unwrap();
                let mut v = Vec::new();
                s.read_to_end(&mut v).unwrap();
                tx.send(v).unwrap();
            }
        });
        (addr, rx)
    }

    /// Starts a TCP server that sends msg to each client when they connect
    /// and then closes the connection
    fn start_tcp_sending_server(msg: String) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let mut s = stream.unwrap();
                s.write_all(msg.as_bytes()).unwrap();
                s.flush().unwrap();
            }
        });
        addr
    }

    /// Starts a TCP echo server and returns the address assigned to the server by the OS
    fn start_tcp_echo_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let mut s = stream.unwrap();
                let mut v = Vec::new();
                s.read_to_end(&mut v).unwrap();
                s.write_all(&v[..]).unwrap();
                s.flush().unwrap();
            }
        });
        addr
    }
}
