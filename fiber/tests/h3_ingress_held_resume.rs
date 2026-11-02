//! Concurrent request bodies on the public H3 server path.
//!
//! All requests are dispatched initially. The scenarios check exact responses
//! and lifecycle accounting for split and consecutive DATA frames; they do not
//! establish a full receive-credit ring at DATA resume.

pub mod common;

use std::{convert::Infallible, num::NonZero};

use common::server::h3::{proof::request::Spec, sessions::Scenario};
use dope::core::driver::life::recovery::Recovery;
use sark_core::{life::session::Disabled, manifold::stream::Boundary};
use sark_h3::{conn::options, manifold::server::deadline};

/// Checks responses, lifecycle accounting, and native receive-route teardown.
fn run<'a>(scenario: Scenario<'a>, protocol: options::Config) {
    scenario.try_run_with(
        protocol,
        Disabled,
        deadline::Disabled,
        |error: Recovery<'_, '_>| -> Infallible {
            panic!("connection recovery: {:?}", error.cause())
        },
        |error| panic!("server diagnostic: {error:?}"),
        |outcome| outcome.unwrap_or_else(|error| panic!("scenario failed: {error:?}")),
    );
}

/// A three-byte request encoded as two DATA frames.
fn split_body_request() -> Spec<'static> {
    let mut request = Spec::new(b"/body", b"abc");
    request.data.split = Some(1);
    request.data.boundary = Boundary::End;
    request
}

#[test]
fn split_body_with_concurrent_requests() {
    let bulk = vec![b'x'; 40 * 1024];
    let scenario = Scenario {
        name: "split-body-concurrent-requests",
        requests: vec![
            split_body_request(),
            Spec::new(b"/body", bulk.as_slice()),
            Spec::new(b"/body", b"tail"),
        ],
        expected_bodies: vec![Some(&b"abc"[..]), Some(bulk.as_slice()), Some(&b"tail"[..])],
        task_capacity: 3,
        cancellation: None,
        must_finish_before_last: None,
        linked: 3,
    };
    run(scenario, options::Config::default());
}

#[test]
fn consecutive_data_frames_preserve_body() {
    let mut request = Spec::new(b"/body", b"abc");
    request.data.chunk_size = Some(NonZero::<usize>::MIN);
    let scenario = Scenario {
        name: "consecutive-data-frames",
        requests: vec![request, Spec::new(b"/body", b"follow-up")],
        expected_bodies: vec![Some(&b"abc"[..]), Some(&b"follow-up"[..])],
        task_capacity: 2,
        cancellation: None,
        must_finish_before_last: None,
        linked: 2,
    };
    run(scenario, options::Config::default());
}

#[test]
fn split_body_with_advertised_qpack_capacity() {
    let bulk = vec![b'y'; 48 * 1024];
    let mut protocol = options::Config::default();
    protocol.qpack.decoder.table_bytes = 4096;
    let mut requests = vec![split_body_request()];
    let mut bodies = vec![Some(&b"abc"[..])];
    for _ in 0..6 {
        requests.push(Spec::new(b"/body", b"dq"));
        bodies.push(Some(&b"dq"[..]));
    }
    requests.push(Spec::new(b"/body", bulk.as_slice()));
    bodies.push(Some(bulk.as_slice()));
    requests.push(Spec::new(b"/body", b"tail"));
    bodies.push(Some(&b"tail"[..]));
    let scenario = Scenario {
        name: "split-body-advertised-qpack-capacity",
        requests,
        expected_bodies: bodies,
        task_capacity: 9,
        cancellation: None,
        must_finish_before_last: None,
        linked: 9,
    };
    run(scenario, protocol);
}
