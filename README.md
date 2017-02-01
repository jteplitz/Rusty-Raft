[![Build Status](https://travis-ci.org/jteplitz602/Rusty-Raft.svg?branch=master)](https://travis-ci.org/jteplitz602/Rusty-Raft)
# Rusty-Raft
An implemention of the [Raft consensus algorithim](https://raft.github.io) in rust. 

## Building
NB: You must have the capnp compiler installed on your system. See the [installation instructions here](https://capnproto.org/install.html) for details.
`cargo build`

## Documentation
You can generate documentation with `cargo doc`. Then load up `target/doc/rusty_raft.index.html` in your browser.

## Testing
`cargo test`

## Module Structure
```
rusty_raft
  |-- rpc
    |-- client
    |-- server
  |-- protocol
```
