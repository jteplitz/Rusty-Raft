pub mod client;
pub mod server;

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
    RpcClientError(RpcClientError),
    Text(String)
}

impl fmt::Display for RpcError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			RpcError::Io(ref err) => write!(f, "IO error: {}", err),
			RpcError::Capnp(ref err) => write!(f, "Parse error: {}", err),
			RpcError::RpcClientError(ref err) => write!(f, "RPC client error: {}", err),
            RpcError::Text(ref s) => write!(f, "{}", s)
		}
	}
}


impl Error for RpcError {
    fn description(&self) -> &str {
        match *self {
            RpcError::Io(ref err) => err.description(),
            RpcError::Capnp(ref err) => err.description(),
            RpcError::RpcClientError(ref err) => err.description(),
            RpcError::Text(ref s) => s
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            RpcError::Io(ref err) => Some(err),
            RpcError::Capnp(ref err) => Some(err),
			RpcError::RpcClientError(ref err) => Some(err),
            RpcError::Text(ref s) => None
        }
    }
}
