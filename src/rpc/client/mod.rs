extern crate capnp;

use std::net::{TcpStream, ToSocketAddrs, Shutdown};
use std::io::{BufWriter, BufReader, Write};
use rpc_capnp::{rpc_request, rpc_response};
use capnp::{serialize_packed, message};
use capnp::serialize::{OwnedSegments};
use super::RpcError;

pub struct Rpc {
    msg: message::Builder<message::HeapAllocator>,
}

impl Rpc {
    ///
    /// Constructs a new Rpc for the given opcode
    /// call Rpc::get_param_builder to add parameters.
    ///
    pub fn new (opcode: i16) -> Rpc {
        let mut msg = message::Builder::new_default(); 
        {
            let mut rpc_request = msg.init_root::<rpc_request::Builder>();
            rpc_request.set_counter(0); // Counter is unused for now
            rpc_request.set_opcode(opcode);
            rpc_request.set_version(1i16);
        }
        Rpc {msg: msg}
    }

    ///
    /// Returns a params::Builder for this request.
    ///
    /// # Panics
    /// Panics if Rpc was not constructed by calling Rpc::new.
    ///
    pub fn get_param_builder (&mut self) -> capnp::any_pointer::Builder {
        self.msg.get_root::<rpc_request::Builder>()
            .map(|root| {
                root.get_params()
            }).unwrap()
    }

    ///
    /// Sends the Rpc to the given server, and blocks until a response is recieved (or it errors
    /// out).
    /// 
    /// # Errors
    /// * Returns an RpcError::Io if there was an Io error while sending the request or recieving 
    /// the response.
    /// * Returns an RpcError::Capnp if there was an issue parsing the RpcResponse header of the
    /// server's resposne message.
    ///
    pub fn send<A: ToSocketAddrs> (&self, addr: A) 
        -> Result<message::Reader<OwnedSegments>, RpcError>
    {
        TcpStream::connect(addr)
        .and_then(|s| {
            let mut writer = BufWriter::new(try!(s.try_clone()));
            serialize_packed::write_message(&mut writer, &self.msg)
            .and_then(move |_| {
                writer.flush()
            })
            .and_then(|_| {
                s.shutdown(Shutdown::Write)
            })
            .map(|_| {
                s
            })
        })
        .map_err(RpcError::Io)
        .and_then(|s| {
            let mut reader = BufReader::new(s);
            serialize_packed::read_message(&mut reader, capnp::message::ReaderOptions::new())
            .map_err(RpcError::Capnp)
        })
    }

    ///
    /// Associated method that exposes the result from the message reader returned by send.
    /// Returns a capnp::any_pointer:Reader which the user should cast to the correct type.
    /// 
    /// # Errors
    /// Returns an RpcError::Capnp if msg is not a valid RpcResponse
    ///
    pub fn get_result_reader (msg: &message::Reader<OwnedSegments>) 
        -> Result<capnp::any_pointer::Reader, RpcError>
    {
        msg.get_root::<rpc_response::Reader>()
        .map(|response| {
            response.get_result()
        })
        .map_err(RpcError::Capnp)
    }
}
