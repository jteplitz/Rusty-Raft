pub mod rpc;
extern crate capnp;

// DO NOT MOVE THIS OR RENAME THIS
// TODO: This is a giant hack, but appears to be the "correct" way to do this
// See: https://github.com/dwrensha/capnpc-rust/issues/5
pub mod rpc_capnp {
    include!(concat!(env!("OUT_DIR"), "/rpc_capnp.rs"));
}
