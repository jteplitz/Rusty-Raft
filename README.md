[![Build Status](https://travis-ci.org/jteplitz602/Rusty-Raft.svg?branch=master)](https://travis-ci.org/jteplitz602/Rusty-Raft)
# Rusty-Raft
A cross-platform, generic implemention of the [Raft consensus algorithim](https://raft.github.io) in rust.

## Building
NB: You must have the capnp compiler installed on your system. See the [installation instructions here](https://capnproto.org/install.html) for details.
Once capnp is installed run:
`cargo build`

## Documentation
You can generate documentation with `cargo doc`. Then load up `target/doc/rusty_raft/index.html` in your browser.

## Testing
To run the full suite of unit and integration tests run:
`cargo test`

## Example
We've implemented an example state machine in `examples/hashmap.rs`, which is a demo of a distributed key value store.
To run it, you must first define your cluster topography in a file. An example `cluster.txt` would look like this:
```
3
1 127.0.0.1:8080
2 127.0.0.1:8081
3 127.0.0.1:8082
```
The first line is the number of servers in the cluster. Each line after that contains a node's id followed by its address.
Then you can start a cluster by running `hashmap servers cluster.txt`, and in a seperate process you can connect
to that cluster with `hashmap client cluster.txt`. NB: If you're using cargo to run the hashmap you'll want to run
it with `cargo run --example hashmap ARGS` where ARGS are either the servers or client args.
This will compile and run the example binary.

## Module Structure
```
rusty_raft
├── client
├── common
├── protocol
├── rpc
│   ├── client
│   ├── server
└── server
    ├── log
    ├── peer
    ├── state_file
    ├── state_machine
```
