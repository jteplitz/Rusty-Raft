extern crate capnp; 
#[cfg(test)]
mod test;

use std::mem;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::error::Error;
use super::{RpcError, RpcClientError, RpcClientErrorKind};

use capnp::{serialize_packed, message};
use capnp::serialize::OwnedSegments;
use rpc_capnp::{rpc_request, rpc_response, rpc_error};
use std::net::{TcpListener, TcpStream, ToSocketAddrs, SocketAddr};
use std::io::{Write, Error as IoError, ErrorKind, BufReader, BufWriter};
use std::thread;
use std::thread::{JoinHandle};
use std::collections::HashMap;

macro_rules! println_stderr(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

///
/// Trait for all objects that handle Rpc requests.
/// RpcObjects must be thread safe.
/// The idea is that any state needed to handle an Rpc should be contained within the RpcObject
/// and stored in a thread safe manner.
///
pub trait RpcObject: Sync + Send {
    ///
    /// Called to handle a single incoming RpcRequest.
    /// The first parameter is a reader for the request's parameters.
    /// The second parameter is a builder where the user should put their response.
    ///
    /// After this function returns the RpcServer will send the response back to the caller.
    /// If the user returns an error then the result is overwritten with the contents of the
    /// error, and the error is sent back to the caller.
    /// Currently the errors are just sent back as strings, but we [will change
    /// this](https://github.com/jteplitz602/Rusty-Raft/issues/3).
    ///
    fn handle_rpc (&self, capnp::any_pointer::Reader, capnp::any_pointer::Builder) -> Result<(), RpcError>;
}

type ServicesMap = HashMap<i16, Box<RpcObject>>;

pub struct RpcServer {
    services: Arc<ServicesMap>,
    listener: Option<TcpListener>,
    repl_thread: Option<JoinHandle<()>>,
    shutting_down: Arc<AtomicBool>
}

impl RpcServer {
    // TODO: It'd be nice if this took a generic iterator over
    // (i16, RpcObject) tuples
    pub fn new_with_services (iter: Vec<(i16, Box<RpcObject>)>) -> RpcServer {
        let mut map = HashMap::new();
        for (opcode, rpc_object) in iter {
            map.insert(opcode.clone(), rpc_object);
        }

        RpcServer {services: Arc::new(map), listener: None, repl_thread: None, shutting_down: Arc::new(AtomicBool::new(false))}
    }

    ///
    /// Binds this server to the given address(es)
    /// At this point the server is accepting requests but you MUST call repl to execute them
    ///
    /// # Panics
    /// Panics if bind has already been called.
    ///
    /// # Errors
    /// Returns a std::io::Error if there is an issue binding the port.
    /// This is typically because the port is already in use.
    ///
    pub fn bind<A: ToSocketAddrs> (&mut self, addr: A) -> Result<(), IoError>{
        if self.listener.is_some() {
            panic!("Bind should only be called once.");
        }

        self.listener = Some(try!(TcpListener::bind(addr)));
        Ok(())
    }

    ///
    /// Spawns a background thread to run a repl loop for this server
    /// This MUST be called to handle incoming requests
    /// You should only call this method once for a given server.
    /// In theory nothing bad should happen if you call it multiple times (besides spawning too
    /// many threads), so the code doesn't stop you from doing this but it is not advised.
    ///
    /// # Panics
    /// Panics if bind has not been called on this server yet
    ///
    /// # Errors
    /// Returns a std::io::Error if there was an issue cloning the listener to send to the
    /// background thread
    ///
    pub fn repl (&mut self) -> Result<(), IoError> {
        let l = match self.listener {
            Some(ref listener) => listener,
            None => panic!("You must call bind before repl.")
        };
        let listener = try!(l.try_clone());

        // TODO #1: Thread pool
        let services = self.services.clone();
        let shutting_down_ref = self.shutting_down.clone();
        thread::spawn(move || {
            // TODO #1: Thread pool
            let mut background_threads = vec![];
            loop {
                let stream = listener.accept();

                match stream {
                    Ok((stream, _)) => {
                        let opcode_map = services.clone();
                        background_threads.push(RpcServer::handle_incoming_connection(opcode_map, stream));
                    }
                    Err(e) => {
                        // shutdown puts the socket in nonblocking mode which will manifest as an
                        // error on accept
                        if shutting_down_ref.load(Ordering::Acquire) {
                            break;
                        }
                        // TODO: Handle TCP errors. Though that may end up meaning being better logging
                        println_stderr!("Error on incoming TCP stream. {}", e)
                    }
                }
            }

            // TODO #1 Thread pool
            // Join each background thread and then exit
            for thread in background_threads {
                match thread.join() {
                    Ok(_) => {},
                    Err(_) => {
                        println_stderr!("Background RPC thread panicked");
                    }
                }
            }

            ()
        });

        Ok(())
    }

    /// 
    /// Returns the local address of the given server.
    /// This is useful if you started a server with port 0 and want to know which port the OS
    /// assigned to the server.
    ///
    /// # Errors
    /// Returns an IoError if the server is not bound to a socket.
    /// 
    pub fn get_local_addr (&self) -> Result<SocketAddr, IoError> {
        match self.listener {
            Some(ref listener) => listener.local_addr(),
            None => return Err(IoError::new(ErrorKind::NotConnected, "Server has not been bound."))
        }
    }

    /// Consumes the RpcServer and blocks until all background threads have been shutdown
    /// and the socket has been released.
    ///
    /// # Panics
    /// Panics if the background thread has panicked
    /// Or if the OS prevents us from waking up the background thread
	pub fn shutdown (self) {
        // Drop self. The drop trait blocks until the server shuts down.
	}
    
    ///
    /// Spawns a new thread to run the function
    /// and sends the result (or error) back to the caller
    ///
    fn handle_incoming_connection (opcode_map: Arc<ServicesMap>, mut stream: TcpStream) -> JoinHandle<()> {
        thread::spawn(move || {
            let mut response_msg = message::Builder::new_default();
            let rpc_result;
            
            { // response_msg_ref scope
                let response_msg_ref = &mut response_msg;
                // TODO(jason) #5: Handle multiple RPCs on the same stream
                rpc_result = RpcServer::wait_for_rpc(&mut stream)
                .and_then(move |reader| {
                    RpcServer::do_rpc(opcode_map, reader, response_msg_ref)
                });
            }

            match rpc_result {
                Ok(_) => RpcServer::send_message(&mut stream, &response_msg),
                Err(e) => RpcServer::send_error(e, &mut stream, &mut response_msg)
            }.unwrap_or_else(|e| {
                // TODO: Handle TCP errors. Though that may end up meaning being better logging
                println_stderr!("Error sending Rpc response: {}", e);
            })
        })
    }

    ///
    /// Sends the given message over the tcp stream.
    ///
    fn send_message<A: message::Allocator> (stream: &mut TcpStream, msg: &message::Builder<A>)
        -> Result<(), IoError>
    {
        // use a buffered writer to avoid extra syscalls.
        let mut writer = BufWriter::new(stream);
        try!(serialize_packed::write_message(&mut writer, msg));
        writer.flush()
    }

    ///
    /// Sends the given error over the tcp stream.
    ///
    fn send_error<A: message::Allocator> (err: RpcError, stream: &mut TcpStream, msg: &mut message::Builder<A>)
            -> Result<(), IoError>
    {
        // The message should already have the counter set, so we get the root and set the error flag
        // and error value

        { // response scope
            // We assume an rpc_response::Builder was passed in. If not panic
            let mut response = msg.get_root::<rpc_response::Builder>().unwrap();
            response.set_error(true);

            let mut result_builder = response.get_result().init_as::<rpc_error::Builder>();
            // TODO #3: Errors shuold be more than just text
            result_builder.set_msg(err.description());
        }
        RpcServer::send_message(stream, msg)
    }

    ///
    /// Waits for an RPC message on the given TCP stream and returns a message reader
    ///
    fn wait_for_rpc(stream: &mut TcpStream) -> Result<message::Reader<OwnedSegments>, RpcError> {
        let mut reader = BufReader::new(stream);
        // create a message reader from the stream
        serialize_packed::read_message(&mut reader, capnp::message::ReaderOptions::new())
        .map_err(RpcError::Capnp)
    }


    /// 
    /// Attempts to parse the given rpc and the opcode, counter value, and parameters.
    ///
    /// # Errors
    /// * Returns an `RpcError::Capnp` due to a malformed rpc request
    /// * Returns an `Rpc::RpcClientError` for an unkown version number
    ///
    fn parse_rpc<'a> (message_reader: &'a message::Reader<OwnedSegments>) -> 
        Result<(i16, i64, capnp::any_pointer::Reader<'a>), RpcError> {
        // Convert that reader to an rpc_request_reader. Returning an RpcError on failure
        let rpc_request_reader = try!(message_reader.get_root::<rpc_request::Reader>()
                .map_err(RpcError::Capnp));

        // Version should always be 1 for now
        let version = rpc_request_reader.get_version();
        if version != 1 {
            let err = Err(RpcClientError {kind: RpcClientErrorKind::UnkownVersion});
            return err.map_err(RpcError::RpcClientError);
        }

        // extract the opcode and params
        let opcode = rpc_request_reader.get_opcode();
        let counter = rpc_request_reader.get_counter();
        let params = rpc_request_reader.get_params();
        Ok((opcode, counter, params))
    }

    ///
    /// Performs the rpc request from the given stream and places the result into
    /// the given message.
    /// It is the caller's responsiblity to set the error bool and errorText in the rpc response
    /// if this function errors out.
    ///
    // HI SYD. The ownership stuff gets interesting in this function
    fn do_rpc<A: message::Allocator> (opcode_map: Arc<HashMap<i16, Box<RpcObject>>>,
                                      reader: message::Reader<OwnedSegments>,
                                      response_msg: &mut message::Builder<A>) 
        -> Result<(), RpcError>
    {
        // create a response message to store the response value and metadata
        let mut response = response_msg.init_root::<rpc_response::Builder>();

        RpcServer::parse_rpc(&reader)
        .and_then(|(opcode, counter, params)| {
            response.set_counter(counter);
            response.set_error(false);

            let unkown_opcode_err = RpcClientError::new(RpcClientErrorKind::UnkownOpcode);

            let mut result = response.init_result();
            opcode_map.get(&opcode)
            .ok_or(RpcError::RpcClientError(unkown_opcode_err))
            .and_then(move |obj| {
                obj.handle_rpc(params, result.borrow())
            })
        })
    }

}

impl Drop for RpcServer {
    /// Gracefully shuts down the RpcServer blocking until all conections are closed
    /// # Panics
    /// Panics if the background thread has panicked
    /// Or if the OS prevents us from waking up the background thread
    fn drop (&mut self) {
        let repl_thread = mem::replace(&mut self.repl_thread, None);
        match repl_thread {
            Some(t) => {
                self.shutting_down.store(true, Ordering::Release);
                // it's not possible to have a repl thread and no tcp listener
                self.listener.as_ref().unwrap().set_nonblocking(true).unwrap();
                t.join().unwrap();
            },
            None => {/* Nothing to shutdown */}
        }
    }
}
