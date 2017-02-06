/// Simple Rpc Module.
/// See the submodules for information on how to set up a server and a client.
pub mod rpc;

///
/// Raft Server
///
pub mod server;
extern crate capnp;
extern crate rand;

// DO NOT MOVE THIS OR RENAME THIS
// TODO: This is a giant hack, but appears to be the "correct" way to do this
// See: https://github.com/dwrensha/capnpc-rust/issues/5
pub mod rpc_capnp {
    include!(concat!(env!("OUT_DIR"), "/rpc_capnp.rs"));
}
pub mod raft_capnp {
    include!(concat!(env!("OUT_DIR"), "/raft_capnp.rs"));
}
