#[macro_use] extern crate matches;
#[macro_use] extern crate log;

/// Simple Rpc Module.
/// See the submodules for information on how to set up a server and a client.
pub mod rpc;

/// Common code between structs.
// TODO: This probably should not all be public
pub mod common;

///
/// Raft Server
///
pub mod server;

///
/// Raft Client libraries
///
pub mod client;

extern crate capnp;
extern crate rand;

// DO NOT MOVE THIS OR RENAME THIS
// TODO: This is a giant hack, but appears to be the "correct" way to do this
// See: https://github.com/dwrensha/capnpc-rust/issues/5
// Need to allow dead code since the capnp compiler outputs a ton of code
#[allow(dead_code)]
mod rpc_capnp {
    include!(concat!(env!("OUT_DIR"), "/rpc_capnp.rs"));
}
#[allow(dead_code)]
mod raft_capnp {
    include!(concat!(env!("OUT_DIR"), "/raft_capnp.rs"));
}
