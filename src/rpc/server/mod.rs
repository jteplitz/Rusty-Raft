extern crate capnp;

#[cfg(test)]
mod test;

use std::sync::{Arc, atomic};
use std::any::Any;
use std::error::Error;

use std::fmt;
use capnp::{serialize_packed, message};
use capnp::serialize::OwnedSegments;
use rpc_capnp::{rpc_request, rpc_response};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::io::{Read, Write, Error as IoError, ErrorKind, BufReader};
use std::thread;
use std::iter::Iterator;
use std::collections::HashMap;
use std::vec;

macro_rules! println_stderr(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

// TODO: Move error types into their own file
#[derive(Debug, PartialEq, Eq)]
pub enum RpcClientErrorKind {
    UnkownVersion,
    UnkownOpcode,
    InvalidParameters
}

#[derive(Debug)]
pub struct RpcClientError {
    pub kind: RpcClientErrorKind
}

impl RpcClientError {
    fn new(kind: RpcClientErrorKind) -> RpcClientError {
        RpcClientError {kind: kind}
    }
}

impl fmt::Display for RpcClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self.kind {
            RpcClientErrorKind::UnkownVersion => "Unkown RPC Version Number",
            RpcClientErrorKind::UnkownOpcode => "Unkown RPC Opcode",
            RpcClientErrorKind::InvalidParameters => "Invalid Parameters for Opcode",
        };
        write!(f, "{}", s)
    }
}

impl Error for RpcClientError {
    fn description(&self) -> &str { 
        // TODO: Don't repeat this code
        match self.kind {
            RpcClientErrorKind::UnkownVersion => "Unkown RPC Version Number",
            RpcClientErrorKind::UnkownOpcode => "Unkown RPC Opcode",
            RpcClientErrorKind::InvalidParameters => "Invalid Parameters for Opcode",
        }
    }
    fn cause (&self) -> Option<&Error> {
        None
    }
}

#[derive(Debug)]
pub enum RpcError {
    Io(IoError),
    Capnp(capnp::Error),
    RpcClientError(RpcClientError)
}

pub type RpcFunction = fn(rpc_request::params::Reader, rpc_response::result::Builder) 
    -> Result<(), RpcError>;

pub trait RpcObject: Sync + Send {
    fn handle_rpc (&self, rpc_request::params::Reader, rpc_response::result::Builder) -> Result<(), RpcError>;
}

pub struct RpcServer {
    services: Arc<HashMap<i16, Box<RpcObject>>>,
    listener: Option<TcpListener>
}

// TODO: Do we register a function or an object with a service?
// It might be cleaner to register a (sendable) object that 

//struct 

/// Attempts to parse the given rpc and the opcode, counter value, and parameters.
/// Can return an error due to an invalid message or unkown version number.
fn parse_rpc<'a> (message_reader: &'a message::Reader<OwnedSegments>) -> 
    Result<(i16, i64, rpc_request::params::Reader<'a>), RpcError> {
    // HEY SYD, THE REFERENCES HERE ARE INTERESTING.
    // I feel like there's a better way to do this...
    // now convert that reader to an rpc_request_reader
    // NB: We can't do this in an and_then closure because we need to keep the original message_reader alive
    // since it shares a lifetime with the rpc_request_reader
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

impl RpcServer {
    ///
    /// Constructs a new RPC server with the given services, but does not start the server.
    /// To start the server call bind
    /// To block and process requests call repl.
    ///
    /// #Example
    /// ```rust,ignore TODO: Don't ignore this
    /// let services = vec!([(0, example_service)])
    /// let server = RpcServer::new_with_services(services);
    /// server.bind(("localhost", port_num)).unwrap();
    /// server.repl();
    /// ```
    // TODO: It'd be nice if this took a generic iterator over
    // (i16, RpcFunction) tuples
    // Also I think this needs to take a Box, but i could be convinced otherwise
    pub fn new_with_services (iter: Vec<(i16, Box<RpcObject>)>) -> RpcServer {
        let mut map = HashMap::new();
        for (opcode, rpc_object) in iter {
            map.insert(opcode.clone(), rpc_object);
        }
        RpcServer {services: Arc::new(map), listener: None}
    }

    ///
    /// Binds this server to the given address(es)
    /// NB: At this point the server is accepting requests but you MUST call repl to run them
    ///
    pub fn bind<A: ToSocketAddrs> (&mut self, addr: A) -> Result<(), IoError>{
        self.listener = Some(try!(TcpListener::bind(addr)));
        Ok(())
    }

    ///
    /// Blocking repl loop for the server
    /// The server will block and handle repl requests 
    /// TODO: How do we shut down...?
    /// Ideally there would be a flag in the struct that represents if shutdown has been called
    /// but I don't think we can have a mutable ref to self in another thread.
    ///
    pub fn repl (&self) -> Result<(), IoError> {
        // TODO: Is there a cleaner way to unwrap this?
        let l = match self.listener {
            Some(ref listener) => listener,
            None => return Err(IoError::new(ErrorKind::NotConnected, "You must call bind before repl."))
        };

        for stream in l.incoming() {
            match stream {
                Ok(stream) => {
                    self.handle_incoming_connection(stream)
                }
                Err(e) => {
                    // TODO: Error handling?
                    println_stderr!("Error on incoming TCP stream. {}", e)
                }
            }
        }

        // never reached...
        Ok(())
    }

    ///
    /// Spawns a new thread to run the function
    /// and sends the result (or error) back to the caller
    ///
    fn handle_incoming_connection(&self, mut stream: TcpStream) {
        let opcode_map = self.services.clone();
        thread::spawn(move || {
            let mut response_msg = message::Builder::new_default();
            
            let response_msg_ref = &mut response_msg;
            // TODO(jason): Handle multiple RPCs on the same stream
            wait_for_rpc(&mut stream)
            .and_then(move |reader| {
                do_rpc(opcode_map, reader, response_msg_ref)
            });
            unimplemented!();
            // TODO: Make sure to use a buffered writer to send the response
            // Unless capnproto already uses one...
        });
    }
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

    parse_rpc(&reader)
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

// TODO: These tests share a lot of code
#[test]
fn it_calls_the_registered_method() {
    let counter = Arc::new(atomic::AtomicUsize::new(0));
    let test_handler = Box::new(test::TestRpcHandler::new_with_counter(counter.clone()));

    let mut opcode_map = HashMap::new();
    opcode_map.insert(0i16, test_handler as Box<RpcObject>);

    // need an Arc pointer to the map for the call signature of do_rpc
    let opcode_map_ref = Arc::new(opcode_map);

    //let server = RpcServer::new_with_services(services);

    // create an addition rpc test message
    let mut message = capnp::message::Builder::new_default();
    {
        let mut rpc_request = message.init_root::<rpc_request::Builder>();
        rpc_request.set_counter(0i64);
        rpc_request.set_opcode(0i16);
        rpc_request.set_version(1i16);
        rpc_request.init_params().init_math();
    }

    // create a response buffer
    let mut response_msg = message::Builder::new_default();

    // easiest way to get a reader is to serialize and deserialize the request
    let mut message_buffer: Vec<u8> = Vec::new();
    serialize_packed::write_message(&mut message_buffer, &message).unwrap();

    let mut buf_reader = &mut BufReader::new(&message_buffer[..]);
    let reader = serialize_packed::read_message(buf_reader, message::ReaderOptions::new()).unwrap();

    do_rpc(opcode_map_ref, reader, &mut response_msg).unwrap();
    assert_eq!(counter.load(atomic::Ordering::SeqCst), 1);
}

#[test]
fn it_rejects_invalid_version_rpcs() {
    let mut opcode_map = HashMap::new();

    // need an Arc pointer to the map for the call signature of do_rpc
    let opcode_map_ref = Arc::new(opcode_map);

    //let server = RpcServer::new_with_services(services);

    // create an addition rpc test message
    let mut message = capnp::message::Builder::new_default();
    {
        let mut rpc_request = message.init_root::<rpc_request::Builder>();
        rpc_request.set_counter(0i64);
        rpc_request.set_opcode(0i16);
        rpc_request.set_version(0i16);
        rpc_request.init_params().init_math();
    }

    // create a response buffer
    let mut response_msg = message::Builder::new_default();

    // easiest way to get a reader is to serialize and deserialize the request
    let mut message_buffer: Vec<u8> = Vec::new();
    serialize_packed::write_message(&mut message_buffer, &message).unwrap();

    let mut buf_reader = &mut BufReader::new(&message_buffer[..]);
    let reader = serialize_packed::read_message(buf_reader, message::ReaderOptions::new()).unwrap();

    let err = match do_rpc(opcode_map_ref, reader, &mut response_msg).unwrap_err() {
        RpcError::RpcClientError(e) => e,
        _ => panic!("Incorrect error type")
    };
    assert_eq!(err.kind, RpcClientErrorKind::UnkownVersion);
}

#[test]
fn it_returns_the_response() {
    const ADDITION_OPCODE: i16  = 5i16;
    const ADDITION_PARAM_1: i32 = 14;
    const ADDITION_PARAM_2: i32 = 57;
    const ADDITION_RESULT: i32  = ADDITION_PARAM_1 + ADDITION_PARAM_2;
    const COUNTER_VAL: i64      = 231267i64;

    let addition_handler = Box::new(test::AdditionRpcHandler {});

    let mut opcode_map = HashMap::new();
    opcode_map.insert(ADDITION_OPCODE, addition_handler as Box<RpcObject>);

    // need an Arc pointer to the map for the call signature of do_rpc
    let opcode_map_ref = Arc::new(opcode_map);

    //let server = RpcServer::new_with_services(services);

    // create an addition rpc test message
    let mut message = capnp::message::Builder::new_default();
    {
        let mut rpc_request = message.init_root::<rpc_request::Builder>();
        rpc_request.set_counter(COUNTER_VAL);
        rpc_request.set_opcode(ADDITION_OPCODE);
        rpc_request.set_version(1i16);
        let mut math_builder = rpc_request.init_params().init_math();
        math_builder.set_num1(ADDITION_PARAM_1);
        math_builder.set_num2(ADDITION_PARAM_2);
    }

    // create a response buffer
    let mut response_msg = message::Builder::new_default();

    // easiest way to get a reader is to serialize and deserialize the request
    let mut message_buffer: Vec<u8> = Vec::new();
    serialize_packed::write_message(&mut message_buffer, &message).unwrap();

    let mut buf_reader = &mut BufReader::new(&message_buffer[..]);
    let reader = serialize_packed::read_message(buf_reader, message::ReaderOptions::new()).unwrap();

    do_rpc(opcode_map_ref, reader, &mut response_msg).unwrap();

    let response_reader = response_msg.get_root_as_reader::<rpc_response::Reader>().unwrap();
    assert_eq!(response_reader.get_counter(), COUNTER_VAL);
    assert_eq!(response_reader.get_error(), false);

    let result_reader = response_reader.get_result();
    assert!(result_reader.has_math());

    let math_reader = match result_reader.which() {
        Ok(rpc_response::result::Which::Math(m)) => m,
        _ => panic!("Invalid result type")
    }.unwrap();
    assert_eq!(math_reader.get_num(), ADDITION_RESULT);
}


///
/// Starts a new echo server that listens for connections in a background thread
/// responds to connections one at a time
/// function returns once echo server has been started and is listening
///
pub fn start_echo_server (port: u16) -> Result<(), IoError>{
    let listener = try!(TcpListener::bind(("localhost", port)));
    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    let _ = do_echo(&mut stream);
                }
                Err(e) => {
                    println_stderr!("{}", e);
                } 
            }
        }
    });
    Ok(())
}

fn do_echo(stream: &mut TcpStream) -> Result<(), IoError>{
    let mut buf: String = String::new();
    stream.read_to_string(&mut buf)
          .and_then(|_| {
            stream.write_all(buf.as_bytes())
          })
}
