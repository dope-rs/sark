pub mod common;

use std::convert::Infallible;

use common::{
    codec::Bytes,
    grpc::{call::egress::reply::Reply, session::buffered::Join, settings::Settings, wire::Peer},
};
use dope_test::harness::server::Pair;
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    Code,
    message::Compression,
    metadata::Metadata,
    protocol::Block,
    server::config::{Input, Limits},
};
use sark_h2::{
    conn::{DataOutcome, config, event::Event},
    frame::{Ack, numbers::ErrorCode},
};

#[test]
fn cancel_and_reuse() {
    let pair = Pair::bind().expect("buffered server");
    let limits = Limits::default()
        .try_calls(2)
        .expect("two call lanes")
        .with_input(Input::try_new(2, 16, 1024, 2048).expect("two buffered messages"));
    let settings = Settings::new(pair.addr(), limits);
    pair.run(
        move |context, listener| {
            let retired = settings
                .try_serve_on(
                    Join,
                    context,
                    listener,
                    common::recover,
                    |result| result.expect("gRPC runtime"),
                )
                .expect("server bank");
            assert!(retired.is_empty(), "unexpected retirement: {retired:?}");
            Ok::<_, Infallible>(())
        },
        |address| {
            let mut peer = Peer::try_config(address, config::Config::default()).expect("peer");
            let headers = Block::request(b"/input/all", None, &Metadata::new())
                .expect("buffered request");
            let pending = peer
                .client
                .streams()
                .headers()
                .request_key(&headers, Boundary::More)
                .expect("first lane");
            peer.client.control().ping(*b"admitted").expect("admission fence");
            peer.try_flush().expect("request headers");
            'admitted: loop {
                peer.try_read().expect("admission response");
                while let Some(event) = peer.client.ingress().poll_event() {
                    if matches!(event, Event::Ping { ack: Ack::Complete, opaque } if opaque == *b"admitted") {
                        break 'admitted;
                    }
                }
            }
            let mut partial = Vec::new();
            Bytes::try_frame(Compression::Plain, b"old", &mut partial).expect("first message");
            partial.extend_from_slice(&[0, 0]);
            let sent = peer
                .client
                .streams()
                .try_send(pending.id(), &partial, Boundary::More)
                .expect("partial buffered request");
            assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == partial.len()));
            peer.try_flush().expect("partial request bytes");

            let reply = Reply::try_reuse(&mut peer, b"/input/one", &[b"other"], true)
                .expect("second lane");
            assert_eq!(reply.status().expect("status").code(), Code::Ok);
            assert_eq!(reply.messages, [b"other".to_vec()]);
            peer.client
                .streams()
                .try_cancel(pending, ErrorCode::Cancel)
                .expect("cancel buffered and partial input");

            for (messages, code, expected) in [
                ([b"new".as_slice(), b"tail".as_slice()].as_slice(), Code::Ok, b"newtail".as_slice()),
                ([b"a".as_slice(), b"b".as_slice(), b"c".as_slice()].as_slice(), Code::ResourceExhausted, b"".as_slice()),
                ([b"again".as_slice(), b"last".as_slice()].as_slice(), Code::Ok, b"againlast".as_slice()),
            ] {
                let reply = Reply::try_reuse(&mut peer, b"/input/all", messages, true)
                    .expect("same connection reuse");
                assert_eq!(reply.status().expect("status").code(), code);
                if code == Code::Ok {
                    assert_eq!(reply.messages, [expected.to_vec()]);
                } else {
                    assert!(reply.messages.is_empty());
                }
            }
            for messages in [&[][..], &[b"a".as_slice(), b"b".as_slice()][..]] {
                let reply = Reply::try_reuse(&mut peer, b"/input/one", messages, true)
                    .expect("single message admission");
                assert_eq!(reply.status().expect("status").code(), Code::InvalidArgument);
                assert!(reply.messages.is_empty());
            }
        },
    )
    .expect("buffered lane lifecycle");
}
