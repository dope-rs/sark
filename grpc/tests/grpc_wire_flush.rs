pub mod common;

use std::{io::Read, net::TcpListener};

use common::grpc::wire::Peer;
use sark_h2::conn::config::Config;

#[test]
fn flushes_wrapped_output() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("flush receiver");
    let mut peer = Peer::try_config(
        listener.local_addr().expect("flush address"),
        Config {
            outbound_capacity: 128,
            ..Config::default()
        },
    )
    .expect("flush sender");
    let (mut receiver, _) = listener.accept().expect("accept sender");
    peer.client.transfer().drain(usize::MAX);
    for byte in 0..10 {
        if byte == 7 {
            peer.client.transfer().drain(100);
        }
        peer.client.control().ping([byte; 8]).expect("queue ping");
    }
    let (first, second) = peer.client.output().slices();
    assert!(!first.is_empty() && !second.is_empty());
    let expected = [first, second].concat();
    peer.try_flush().expect("flush both slices");
    assert!(peer.client.output().is_empty());
    drop(peer);
    let mut received = Vec::new();
    receiver
        .read_to_end(&mut received)
        .expect("complete output");
    assert_eq!(received, expected);
}
