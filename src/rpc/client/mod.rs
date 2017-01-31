extern crate capnp;

use std::net::{TcpStream, ToSocketAddrs};
use std::io::{BufWriter, BufReader, Write};
use rpc_capnp::{rpc_request, rpc_response};
use capnp::{serialize_packed, message};
use capnp::serialize::{OwnedSegments};
use super::RpcError;

///
/// Used to construct and send Rpc to a remote server.
/// The usage is a bit more complicated than is ideal due to capnproto 
/// 
/// TODO: Get this example code to compile
/// # Examples
///
/// ```rust,ignore
/// #let opcode = 0i16;
/// let rpc = Rpc::new(opcode);
/// let param_builder = rpc.get_param_builder();
/// // construct params here
/// rpc.send(("192.168.0.15", 8080))
/// .and_then(|msg| {
///     Rpc::get_result_reader(msg)
///     .map(|result| {
///         // handle result here
///     })
/// });
/// ```
///
pub struct Rpc {
    msg: message::Builder<message::HeapAllocator>,
}

impl Rpc {
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
    /// Returns a capnp::Error if Rpc::new was not called first
    ///
    pub fn get_param_builder (&mut self) -> Result<capnp::any_pointer::Builder, capnp::Error> {
        self.msg.get_root::<rpc_request::Builder>()
            .map(|root| {
                root.get_params()
            })
    }

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
            .map(move |_| {
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
