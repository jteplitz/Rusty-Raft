extern crate capnp; 
#[cfg(test)]
mod test;

use std::sync::{Arc};
use std::error::Error;
use super::{RpcError, RpcClientError, RpcClientErrorKind};

use capnp::{serialize_packed, message};
use capnp::serialize::OwnedSegments;
use rpc_capnp::{rpc_request, rpc_response};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::io::{Write, Error as IoError, ErrorKind, BufReader, BufWriter};
use std::thread;
use std::collections::HashMap;

macro_rules! println_stderr(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

pub trait RpcObject: Sync + Send {
    fn handle_rpc (&self, rpc_request::params::Reader, rpc_response::result::Builder) -> Result<(), RpcError>;
}

type ServicesMap = HashMap<i16, Box<RpcObject>>;

pub struct RpcServer {
    services: Arc<ServicesMap>,
    listener: Option<TcpListener>,
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
    // (i16, RpcObject) tuples
    pub fn new_with_services (iter: Vec<(i16, Box<RpcObject>)>) -> RpcServer {
        let mut map = HashMap::new();
        for (opcode, rpc_object) in iter {
            map.insert(opcode.clone(), rpc_object);
        }

        RpcServer {services: Arc::new(map), listener: None}
    }

    ///
    /// Binds this server to the given address(es)
    /// At this point the server is accepting requests but you MUST call repl to execute them
    ///
    pub fn bind<A: ToSocketAddrs> (&mut self, addr: A) -> Result<(), IoError>{
        if self.listener.is_some() {
            return Err(IoError::new(ErrorKind::AlreadyExists, "Server is already bound"));
        }

        self.listener = Some(try!(TcpListener::bind(addr)));
        Ok(())
    }

    ///
    /// Spawns a background thread to run a repl loop for this server
    /// This MUST be called to handle incoming requests
    /// In theory it should be fine to call this multiple times, but there's no to do so
    ///
    pub fn repl (&mut self) -> Result<(), IoError> {
        let l = match self.listener {
            Some(ref listener) => listener,
            None => return Err(IoError::new(ErrorKind::NotConnected, "You must call bind before repl."))
        };
        let listener = try!(l.try_clone());

        // TODO #1: Thread pool
        let services = self.services.clone();
        thread::spawn(move || {
            loop {

                let stream = listener.accept();
                let opcode_map = services.clone();

                match stream {
                    Ok((stream, _)) => {
                        RpcServer::handle_incoming_connection(opcode_map, stream);
                    }
                    Err(e) => {
                        // TODO: Handle TCP errors. Though that may end up meaning being better logging
                        println_stderr!("Error on incoming TCP stream. {}", e)
                    }
                }
            }
        });

        Ok(())
    }

    // TODO #2: Handle shutdown
    #[allow(dead_code)]
	fn shutdown (&mut self) {
        unimplemented!();
	}
    
    ///
    /// Spawns a new thread to run the function
    /// and sends the result (or error) back to the caller
    ///
    fn handle_incoming_connection (opcode_map: Arc<ServicesMap>, mut stream: TcpStream) {
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
        });
    }

    fn send_message<A: message::Allocator> (stream: &mut TcpStream, msg: &message::Builder<A>)
        -> Result<(), IoError>
    {
        // use a buffered writer to avoid extra syscalls.
        let mut writer = BufWriter::new(stream);
        try!(serialize_packed::write_message(&mut writer, msg));
        writer.flush()
    }

    fn send_error<A: message::Allocator> (err: RpcError, stream: &mut TcpStream, msg: &mut message::Builder<A>)
            -> Result<(), IoError>
    {
        // The message should already have the counter set, so we get the root and set the error flag
        // and error value

        { // response scope
            // We assume an rpc_response::Builder was passed in. If not panic
            let mut response = msg.get_root::<rpc_response::Builder>().unwrap();
            response.set_error(true);

            let mut result_builder= response.init_result();
            // TODO #3: Errors shuold be more than just text
            result_builder.set_error_text(err.description());
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


    /// Attempts to parse the given rpc and the opcode, counter value, and parameters.
    /// Can return an error due to an invalid message or unkown version number.
    fn parse_rpc<'a> (message_reader: &'a message::Reader<OwnedSegments>) -> 
        Result<(i16, i64, rpc_request::params::Reader<'a>), RpcError> {
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

/************************/
/*   BEGIN UNIT TESTS   */
/************************/
// TODO: These tests share a lot of code

#[cfg(test)]
use std::sync::atomic;
#[cfg(test)]
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

    RpcServer::do_rpc(opcode_map_ref, reader, &mut response_msg).unwrap();
    assert_eq!(counter.load(atomic::Ordering::SeqCst), 1);
}

#[cfg(test)]
#[test]
fn it_rejects_invalid_version_rpcs() {
    let opcode_map = HashMap::new();

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

    let err = match RpcServer::do_rpc(opcode_map_ref, reader, &mut response_msg).unwrap_err() {
        RpcError::RpcClientError(e) => e,
        _ => panic!("Incorrect error type")
    };
    assert_eq!(err.kind, RpcClientErrorKind::UnkownVersion);
}

#[cfg(test)]
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

    // create an addition rpc test message
	let message = test::create_test_addition_rpc(COUNTER_VAL, ADDITION_OPCODE, ADDITION_PARAM_1, ADDITION_PARAM_2);

    // create a response buffer
    let mut response_msg = message::Builder::new_default();

    // easiest way to get a reader is to serialize and deserialize the request
    let mut message_buffer: Vec<u8> = Vec::new();
    serialize_packed::write_message(&mut message_buffer, &message).unwrap();

    let mut buf_reader = &mut BufReader::new(&message_buffer[..]);
    let reader = serialize_packed::read_message(buf_reader, message::ReaderOptions::new()).unwrap();

    RpcServer::do_rpc(opcode_map_ref, reader, &mut response_msg).unwrap();

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
