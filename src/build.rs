extern crate capnpc;

fn main() {
    println!("Build called");
    ::capnpc::CompilerCommand::new()
        .src_prefix("src/protocol")
        .file("src/protocol/rpc.capnp")
        .run()
        .expect("schema compiler command");
}
