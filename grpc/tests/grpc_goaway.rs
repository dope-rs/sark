use crate::common::grpc::call::{Open as _, calls::Settings};
pub mod common;
use sark_grpc::{
    Code,
    client::{call::unary::Session as _, io::ingress::Session as _, session},
    metadata::Metadata,
    protocol::section,
};
use sark_h2::{client, frame::numbers::ErrorCode};

use crate::common::grpc::session::goaways::{Last as _, Server as _, try_peer};

#[test]
fn graceful() {
    let limits = Settings::new(Some(2), Some(2), None, None)
        .try_build()
        .expect("GOAWAY fixture limits");
    session::Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let calls = client.try_requests(2).expect("GOAWAY fixture");
            let (mut server, streams) = try_peer(&mut client).expect("GOAWAY peer");
            assert_eq!(streams.len(), 2);

            client
                .ingest(
                    &streams[0]
                        .id()
                        .try_goaway(ErrorCode::NoError)
                        .expect("GOAWAY fixture"),
                )
                .expect("ingest graceful GOAWAY");
            assert!(matches!(
                client.poll(&calls[1]),
                Err(status) if status.code() == Code::Unavailable
            ));
            assert!(
                client
                    .poll(&calls[0])
                    .expect("poll accepted pre-GOAWAY call")
                    .is_none()
            );
            assert!(matches!(
                client.start_raw(b"/svc/New", None, &Metadata::new(), b"request"),
                Err(status) if status.code() == Code::Unavailable
            ));

            server.try_finish(streams[0]).expect("GOAWAY fixture");
            client
                .ingest(server.output().first())
                .expect("ingest accepted response after graceful GOAWAY");
            let response = client
                .poll(&calls[0])
                .expect("poll accepted response after graceful GOAWAY")
                .expect("accepted response completes");
            assert_eq!(response.status.code(), Code::Ok);
        },
    )
    .expect("graceful GOAWAY session");
}

#[test]
fn error() {
    let limits = Settings::new(Some(2), Some(2), None, None)
        .try_build()
        .expect("GOAWAY fixture limits");
    session::Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let calls = client.try_requests(2).expect("GOAWAY fixture");
            let (_, streams) = try_peer(&mut client).expect("GOAWAY peer");
            client
                .ingest(
                    &streams[1]
                        .id()
                        .try_goaway(ErrorCode::ProtocolError)
                        .expect("GOAWAY fixture"),
                )
                .expect("ingest error GOAWAY");
            for call in calls {
                assert!(matches!(
                    client.poll(&call),
                    Err(status) if status.code() == Code::Unavailable
                ));
            }
        },
    )
    .expect("error GOAWAY session");
}

#[test]
fn replay() {
    let limits = Settings::new(Some(6), Some(1), None, None)
        .try_build()
        .expect("GOAWAY fixture limits");
    session::Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let mut calls = client.try_requests(6).expect("GOAWAY fixture");
            let (mut server, streams) = try_peer(&mut client).expect("GOAWAY peer");
            client
                .ingest(
                    &streams[0]
                        .id()
                        .try_goaway(ErrorCode::NoError)
                        .expect("GOAWAY fixture"),
                )
                .expect("ingest completion-blocked GOAWAY");
            assert!(client.is_blocked());

            client.cancel(calls.remove(2));
            let accepted = calls.remove(0);
            for call in calls {
                assert!(matches!(
                    client.poll(&call),
                    Err(status) if status.code() == Code::Unavailable
                ));
            }
            assert!(!client.is_blocked());
            server.try_finish(streams[0]).expect("GOAWAY fixture");
            client
                .ingest(server.output().first())
                .expect("accepted response");
            let response = client
                .poll(&accepted)
                .expect("accepted call")
                .expect("completion");
            assert_eq!(response.status.code(), Code::Ok);
        },
    )
    .expect("completion-backpressured GOAWAY session");
}
