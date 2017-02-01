/// Used to construct and send an Rpc to a remote server.
/// 
/// # Examples
///
/// Constructing and sending an RPC:
///
/// ```rust,no_run
/// # let opcode = 0i16;
/// # let my_server_ip = "192.168.0.1";
/// # let my_server_port = 8080u16;
/// # use rusty_raft::rpc::client::Rpc;
/// let mut rpc = Rpc::new(opcode);
/// {
///     // We need to scope the creation of the parameters here
///     // because the parameter builder borrows a mutable reference to the rpc
///     // object.
///     let param_builder = rpc.get_param_builder();
///     // construct params here. Cast param_builder to the correct type by doing
///     // param_builder.init_as::<my_type::Builder>();
/// }
/// rpc.send((my_server_ip, my_server_port))
/// .and_then(|msg| {
///     Rpc::get_result_reader(&msg)
///     .map(|result| {
///         // handle result here. Cast result to the correct type by doing
///         // result.get_as::<my_type::Reader>(),
///         // which returns a Result<my_type::Reader, capnp::Error>
///     })
/// });
/// ```
pub mod client;
/// Handles incoming Rpc requests and sends out responses.
///
/// #Examples
/// Starting an Rpc server to handle a single type of request
///
/// ```rust
/// # extern crate capnp;
/// # extern crate rusty_raft;
/// # fn main() {
/// # use rusty_raft::rpc::server::{RpcServer, RpcObject};
/// # use rusty_raft::rpc::RpcError;
/// // Define the Rpc handler for our request type
/// struct MyRpcHandler {
///     // Any state needed to handle an Rpc should be stored here
/// }
/// 
/// impl RpcObject for MyRpcHandler {
///     fn handle_rpc (&self, params: capnp::any_pointer::Reader, result: capnp::any_pointer::Builder) 
///         -> Result<(), RpcError> {
///             // perform Rpc here, and place the results into the result builder.
///             // return Ok(()) on success or an RpcError on error.
///             # Ok(())
///     }
/// }
///
/// // Create an instance of the Rpc handler to handle all Rpcs and cast it to a trait object
/// let my_rpc_handler: Box<RpcObject> = Box::new(MyRpcHandler {});
/// # let my_opcode = 0i16;
/// let services = vec![(my_opcode, my_rpc_handler)];
/// let mut server = RpcServer::new_with_services(services);
/// # let port_num = 0u16;
/// server.bind(("localhost", port_num)).unwrap();
/// server.repl().unwrap();
/// # }
/// ```
pub mod server;
#[cfg(test)]
mod test;

extern crate capnp;
use std::io::{Error as IoError};
use std::error::Error;
use std::fmt;


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

impl fmt::Display for RpcError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			RpcError::Io(ref err) => write!(f, "IO error: {}", err),
			RpcError::Capnp(ref err) => write!(f, "Parse error: {}", err),
			RpcError::RpcClientError(ref err) => write!(f, "RPC client error: {}", err)
		}
	}
}


impl Error for RpcError {
    fn description(&self) -> &str {
        match *self {
            RpcError::Io(ref err) => err.description(),
            RpcError::Capnp(ref err) => err.description(),
            RpcError::RpcClientError(ref err) => err.description()
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            RpcError::Io(ref err) => Some(err),
            RpcError::Capnp(ref err) => Some(err),
			RpcError::RpcClientError(ref err) => Some(err)
        }
    }
}
