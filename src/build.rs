extern crate capnpc;

fn main() {
    ::capnpc::CompilerCommand::new()
        .src_prefix("src/protocol")
        .file("src/protocol/rpc.capnp")
        .run()
        .expect("schema compiler command");

    ::capnpc::CompilerCommand::new()
        .src_prefix("src/protocol")
        .file("src/protocol/raft.capnp")
        .run()
        .expect("schema compiler command");
}
