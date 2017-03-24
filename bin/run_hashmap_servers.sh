#!/bin/sh

RUST_BACKTRACE=1 RUST_LOG=rusty_raft=$1 ./target/debug/examples/hashmap servers cluster.txt
