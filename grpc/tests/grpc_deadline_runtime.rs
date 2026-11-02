pub mod common;

use std::{convert::Infallible, thread, time::Duration};

use common::grpc::{
    call::egress::capacity,
    settings::Settings,
    wire::{
        Peer,
        deadline::{self, Deferred, Direct, Facts},
    },
};
use dope_test::harness::server::Pair;
use sark_core::manifold::stream::Boundary;
use sark_grpc::{Code, server::config::Limits};
use sark_h2::{
    conn::{DataOutcome, config},
    frame::numbers::ErrorCode,
};

#[test]
fn expiry() {
    for (path, timeout, window, starts, bytes, code, reset) in [
        (
            b"/deadline/stalled".as_slice(),
            b"0n".as_slice(),
            None,
            0,
            0,
            Some(Code::DeadlineExceeded),
            None,
        ),
        (
            b"/deadline/queued",
            b"99999999H",
            None,
            1,
            9,
            Some(Code::Ok),
            None,
        ),
        (
            b"/deadline/stalled",
            b"50m",
            None,
            1,
            0,
            Some(Code::DeadlineExceeded),
            None,
        ),
        (
            b"/deadline/queued",
            b"50m",
            Some(0),
            1,
            0,
            Some(Code::DeadlineExceeded),
            None,
        ),
        (
            b"/deadline/queued",
            b"200m",
            Some(1),
            1,
            1,
            None,
            Some(ErrorCode::InternalError),
        ),
    ] {
        let pair = Pair::bind().expect("deadline listener");
        let settings = Settings::new(
            pair.addr(),
            Limits::default().try_calls(1).expect("one call"),
        );
        pair.run(
            move |context, listener| {
                let facts = Facts::default();
                let retired = settings
                    .try_serve_on(
                        Direct(&facts),
                        context,
                        listener,
                        common::recover,
                        |result| {
                            result.expect("deadline runtime");
                        },
                    )
                    .expect("server resources");
                assert!(retired.is_empty(), "deadline cleanup: {retired:?}");
                assert_eq!(facts.starts.get(), starts);
                assert_eq!(facts.messages.get(), 0, "incomplete message was delivered");
                assert_eq!(facts.drops.get(), starts, "retained handler lifetime");
                Ok::<_, Infallible>(())
            },
            move |address| {
                let mut peer =
                    Peer::try_config(address, config::Config::default()).expect("deadline peer");
                if let Some(window) = window {
                    peer.try_flush().expect("client settings");
                    capacity::try_window(&mut peer.socket, window)
                        .expect("advertised response window");
                }
                let fields = deadline::try_fields(path, Some(timeout)).expect("request fields");
                let stream = peer
                    .client
                    .streams()
                    .headers()
                    .request(&fields, Boundary::More)
                    .expect("request");
                if path == b"/deadline/stalled" {
                    let sent = peer
                        .client
                        .streams()
                        .try_send(stream, &[0; 3], Boundary::More)
                        .expect("incomplete request prefix");
                    assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == 3));
                }
                let replies =
                    deadline::try_replies(&mut peer, &[stream]).expect("deadline response");
                assert_eq!(replies[0].code, code);
                assert_eq!(replies[0].reset, reset);
                assert_eq!(
                    replies[0].body.len(),
                    bytes,
                    "actual bytes before terminal event"
                );
            },
        )
        .expect("deadline lifecycle");
    }
}

#[test]
fn deferred() {
    let pair = Pair::bind().expect("deferred listener");
    let settings = Settings::new(pair.addr(), Limits::default());
    pair.run(
        move |context, listener| {
            let facts = Facts::default();
            let retired = settings
                .try_serve_on(
                    Deferred(Direct(&facts)),
                    context,
                    listener,
                    common::recover,
                    |result| {
                        result.expect("deferred runtime");
                    },
                )
                .expect("server resources");
            assert!(retired.is_empty(), "deferred cleanup: {retired:?}");
            assert_eq!(
                facts.outputs.get(),
                1,
                "deferred callback after request expiry"
            );
            assert_eq!(facts.drops.get(), 1);
            assert!(facts.error.get().is_none());
            Ok::<_, Infallible>(())
        },
        |address| {
            let mut peer =
                Peer::try_config(address, config::Config::default()).expect("deferred peer");
            let fields =
                deadline::try_fields(b"/deadline/deferred", Some(b"100m")).expect("fields");
            let stream = peer
                .client
                .streams()
                .headers()
                .request(&fields, Boundary::More)
                .expect("request");
            let replies = deadline::try_replies(&mut peer, &[stream])
                .expect("request timer wakes before output timer");
            assert_eq!(replies[0].code, Some(Code::DeadlineExceeded));
            assert!(replies[0].body.is_empty());
        },
    )
    .expect("independent deadlines");
}

#[test]
fn reuse() {
    let pair = Pair::bind().expect("reuse listener");
    let settings = Settings::new(
        pair.addr(),
        Limits::default().try_calls(1).expect("one call"),
    );
    pair.run(
        move |context, listener| {
            let facts = Facts::default();
            let retired = settings
                .try_serve_on(
                    Direct(&facts),
                    context,
                    listener,
                    common::recover,
                    |result| {
                        result.expect("reuse runtime");
                    },
                )
                .expect("server resources");
            assert!(retired.is_empty(), "reused deadline cleanup: {retired:?}");
            assert_eq!((facts.starts.get(), facts.drops.get()), (3, 3));
            Ok::<_, Infallible>(())
        },
        |address| {
            let mut peer =
                Peer::try_config(address, config::Config::default()).expect("reuse peer");
            for (index, timeout, boundary) in [
                (0, Some(b"50m".as_slice()), Boundary::End),
                (1, None, Boundary::More),
                (2, Some(b"50m".as_slice()), Boundary::More),
            ] {
                let fields = deadline::try_fields(b"/deadline/reused", timeout).expect("fields");
                let stream = peer
                    .client
                    .streams()
                    .headers()
                    .request(&fields, boundary)
                    .expect("reused request");
                if index == 1 {
                    peer.try_flush().expect("untimed request starts");
                    thread::sleep(Duration::from_millis(100));
                    assert!(matches!(
                        peer.client
                            .streams()
                            .try_send(stream, &[], Boundary::End)
                            .expect("untimed request end"),
                        DataOutcome::Ended
                    ));
                }
                let replies = deadline::try_replies(&mut peer, &[stream]).expect("reused response");
                assert_eq!(
                    replies[0].code,
                    Some(if index == 2 {
                        Code::DeadlineExceeded
                    } else {
                        Code::Ok
                    })
                );
            }
        },
    )
    .expect("generation-aware deadline retirement");
}
