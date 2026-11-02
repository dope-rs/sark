pub mod common;

use std::{cell::Cell, num::NonZero};

use common::{codec::Bytes, grpc::wire::progress::Plan};
use sark_core::manifold::client::domain;
use sark_grpc::{
    Code,
    client::{
        call::{request, unary},
        io::{Session as _, ingress::Session as _},
        session::Session,
    },
    details::{Diagnostic, RequestError},
    metadata::Metadata,
};
use sark_h2::stream;

#[test]
fn rejects_initial_count() {
    for raw in [true, false] {
        let attempts = Cell::new(0);
        let plan = Plan::new(&[7], &attempts, false).expect("carrier plan");
        Session::with(plan, |mut client| {
            let owners = client.owner_cap();
            let status = if raw {
                unary::Session::start_raw(&mut client, b"/svc/Call", None, &Metadata::new(), b"x")
            } else {
                unary::Session::try_start(
                    &mut client,
                    b"/svc/Call",
                    None,
                    &Metadata::new(),
                    &mut Bytes,
                    &b"x".to_vec(),
                )
                .into_result()
            }
            .err()
            .expect("six bytes cannot accept seven");
            let (required, capacity) = if raw { (2, 1) } else { (7, 6) };
            let expected = (NonZero::new(required).expect("nonzero count"), capacity);
            assert!(matches!(
                Diagnostic::from(&status),
                Diagnostic::RequestProgress(domain::Failure::Turn(RequestError::Progress(error)))
                    if error.into_parts() == expected
            ));
            assert_eq!(status.code(), Code::Internal);
            assert_eq!(client.owner_cap(), owners);
        })
        .expect("session");
        assert_eq!(attempts.get(), 1);
    }
}

#[test]
fn rejects_end_count() {
    let attempts = Cell::new(0);
    let plan = Plan::new(&[1], &attempts, false).expect("carrier plan");
    Session::with(plan, |mut client| {
        let stream =
            request::Session::open(&mut client, b"/progress/write", None, &Metadata::new())
                .expect("request");
        let status = request::Session::finish(&mut client, stream)
            .err()
            .expect("empty END cannot accept one");
        assert!(matches!(
            Diagnostic::from(&status),
            Diagnostic::RequestProgress(domain::Failure::Turn(RequestError::Progress(error)))
                if error.into_parts() == (NonZero::<usize>::MIN, 0)
        ));
        assert_eq!(status.code(), Code::Internal);
        let next = request::Session::open(&mut client, b"/progress/write", None, &Metadata::new())
            .expect("call slot released");
        request::Session::cancel(&mut client, next);
    })
    .expect("session");
    assert_eq!(attempts.get(), 1);
}

#[test]
fn rejects_resumed_count() {
    for stale in [false, true] {
        let attempts = Cell::new(0);
        let plan = Plan::new(&[1, usize::MAX], &attempts, stale).expect("carrier plan");
        Session::with(plan, |mut client| {
            let owners = client.owner_cap();
            let call = unary::Session::try_start(
                &mut client,
                b"/svc/Call",
                None,
                &Metadata::new(),
                &mut Bytes,
                &b"x".to_vec(),
            )
            .into_result()
            .expect("one admitted byte");
            let status = client.resume().expect_err("overflowing carrier count");
            assert_eq!(status.code(), Code::Internal);
            if stale {
                assert!(matches!(
                    Diagnostic::from(&status),
                    Diagnostic::RequestProgress(domain::Failure::Settle {
                        terminal: RequestError::Progress(error),
                        error: key,
                    }) if error.into_parts() == (NonZero::<usize>::MAX, 5)
                        && key.id() == stream::Id::FIRST_CLIENT
                ));
            } else {
                assert!(matches!(
                    Diagnostic::from(&status),
                    Diagnostic::RequestProgress(domain::Failure::Turn(RequestError::Progress(error)))
                        if error.into_parts() == (NonZero::<usize>::MAX, 5)
                ));
            }
            assert_eq!(client.owner_cap(), owners);
            unary::Session::cancel(&mut client, call);
        })
        .expect("session");
        assert_eq!(attempts.get(), 2);
    }
}

#[test]
fn preserves_partial_writes() {
    for written in [&[1, 0, 6][..], &[6, 1][..]] {
        let attempts = Cell::new(0);
        let plan = Plan::new(written, &attempts, false).expect("carrier plan");
        Session::with(plan, |mut client| {
            let owners = client.owner_cap();
            let call =
                unary::Session::start_raw(&mut client, b"/svc/Call", None, &Metadata::new(), b"xy")
                    .expect("partial request");
            for _ in &written[1..] {
                client.resume().expect("admitted remaining bytes");
            }
            assert_eq!(client.owner_cap(), owners);
            unary::Session::cancel(&mut client, call);
        })
        .expect("session");
        assert_eq!(attempts.get(), written.len());
    }
}
