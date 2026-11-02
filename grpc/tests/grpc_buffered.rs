pub mod common;

use std::{cell::Cell, convert::Infallible};

use common::grpc::{call::egress::reply::Reply, session::Echo, settings::Settings};
use dope_test::{alloc::Tracker, harness::server::Pair};
use sark_grpc::{Code, server::config::Limits};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn response_per_turn() {
    let pair = Pair::bind().expect("buffered server");
    let settings = Settings::new(pair.addr(), Limits::default());
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
        },
    )
    .expect("buffered writer contract");
}
