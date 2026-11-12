pub mod common;

use std::{cell::Cell, convert::Infallible};

use common::grpc::{
    call::egress::reply::Reply,
    session::Echo,
    settings::Settings,
    wire::{Peer, deadline},
};
use dope_test::{alloc::Tracker, harness::server::Pair};
use sark_core::manifold::stream::Boundary;
use sark_grpc::{Code, metadata::Metadata, protocol::request::Fields, server::config::Limits};
use sark_http::field;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn response_per_turn() {
    let pair = Pair::bind().expect("buffered server");
    let settings = Settings::new(
        pair.addr(),
        Limits::default().try_calls(1).expect("one call"),
    );
    pair.run(
        move |context, listener| {
            let rejected = Cell::new(None);
            let retired = settings
                .try_serve_on(
                    Echo(&rejected),
                    context,
                    listener,
                    common::recover,
                    |result| result.expect("gRPC runtime"),
                )
                .expect("server bank");
            assert!(
                retired.is_empty(),
                "unexpected server retirement: {retired:?}"
            );
            assert_eq!(
                rejected.get(),
                Some(0),
                "rejection must precede owner allocation"
            );
            Ok::<_, Infallible>(())
        },
        |address| {
            let reply =
                Reply::try_exchange(address, b"/test/svc", &[b"one"], true).expect("unary echo");
            assert_eq!(reply.status().expect("response status").code(), Code::Ok);
            assert_eq!(reply.messages, [b"one".to_vec()]);
            let reply = Reply::try_exchange(address, b"/test/svc", &[b"one", b"two"], true)
                .expect("excess response");
            assert_eq!(
                reply.status().expect("response status").code(),
                Code::Internal
            );
            assert!(reply.messages.is_empty());
            let mut peer = Peer::try_config(address, Default::default()).expect("HEAD end peer");
            for (path, initial, code) in [
                (b"/test/prepare".as_slice(), true, Code::Internal),
                (b"/test/svc".as_slice(), true, Code::Ok),
                (b"/test/svc".as_slice(), false, Code::Ok),
            ] {
                let metadata = Metadata::new();
                let fields =
                    Fields::new(path, Some(b"localhost"), &metadata).expect("request fields");
                let stream = peer
                    .client
                    .streams()
                    .headers()
                    .request(
                        fields,
                        if initial {
                            Boundary::End
                        } else {
                            Boundary::More
                        },
                    )
                    .expect("initial request head");
                if !initial {
                    peer.client
                        .streams()
                        .headers()
                        .trail(
                            stream,
                            [field::Entry::new(
                                field::literal!(field::Lower::try_bytes(b"x-request-trailer")),
                                field::literal!(field::Value::try_bytes(b"retained")),
                            )],
                        )
                        .expect("request trailer ends input");
                }
                let replies = deadline::try_replies(&mut peer, &[stream]).expect("HEAD end reply");
                assert_eq!(replies[0].code, Some(code));
                assert!(replies[0].body.is_empty());
            }
        },
    )
    .expect("buffered writer contract");
}
