pub mod common;

use std::{cell::Cell, fmt, num::NonZero};

use common::h2::{
    apps::{NESTED_JSON, RETAINED_NAME},
    events::Log,
    facts::{GuardProbe, State},
    peer::{Peer, try_send},
    server::{CONFIG, Request, Server, check},
};

use dope_test::harness::server::Pair;
use sark_core::{
    life::mode::{self, Observed},
    manifold::stream::Boundary,
};
use sark_http::field::{self, Entry, Lower, Value};

#[test]
fn rejects_atomically() {
    let harness = Pair::bind().expect("H2 harness");
    let mut config = CONFIG;
    config.max_outbound_bytes = 1;
    let mut facts = Vec::new();

    harness
        .check(
            move |context, listener| {
                let log = Cell::new(Vec::new());
                Server::new(config, 2, State::default())
                    .try_run(
                        context,
                        listener,
                        Observed::new(Log(&log, |cause: &dyn fmt::Debug| {
                            eprintln!("lifecycle diagnostic: {cause:?}");
                        })),
                    )
                    .map(|()| log.into_inner())
            },
            |address| {
                let (mut socket, mut client) = Peer::connect(address);
                let mut peer = Peer::new(&mut socket, &mut client);
                let first = Request::new(b"/missing-first")
                    .try_start(peer.client, Boundary::End)
                    .expect("start first rejected head");
                let second = Request::new(b"/missing-second")
                    .try_start(peer.client, Boundary::End)
                    .expect("start second rejected head");
                peer.try_flush().expect("H2 peer flush");
                let (_, bodies) = peer
                    .try_statuses(&[first, second], b"404")
                    .expect("H2 peer statuses");
                assert!(bodies.iter().all(Vec::is_empty));
            },
            |result| {
                facts = result.expect("H2 setup");
            },
        )
        .expect("H2 rejected-head backpressure runtime");

    let streams = facts
        .iter()
        .filter_map(|&(kind, stream)| (kind == "exchange-opened").then_some(stream))
        .collect::<Vec<_>>();
    assert_eq!(streams.len(), 2);
    for stream in streams {
        let actual = facts
            .iter()
            .filter_map(|&(kind, source)| (source == stream).then_some(kind))
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            [
                "exchange-opened",
                "input-finished",
                "response-committed",
                "request-owner-scope-closed",
                "exchange-rejected",
                "terminal",
                "exchange-retired",
            ],
        );
    }
}

#[test]
fn retains_segmented_json() {
    check(CONFIG, 8, |peer| {
        let stream = Request::new(b"/nested")
            .try_start(peer.client, Boundary::More)
            .expect("start H2 request");
        let body = NESTED_JSON;
        for (index, segment) in body.chunks(11).enumerate() {
            try_send(
                peer.client,
                stream,
                segment,
                if (index + 1) * 11 >= body.len() {
                    Boundary::End
                } else {
                    Boundary::More
                },
            )
            .expect("H2 peer send");
            peer.try_flush().expect("H2 peer flush");
        }
        let (headers, response) = peer.try_response(stream).expect("H2 peer response");
        assert_eq!(
            headers
                .iter()
                .find(|field| field.name == b":status")
                .map(|field| field.value.as_slice()),
            Some(&b"200"[..]),
        );
        assert_eq!(response, b"retained");
    })
    .expect("H2 task runtime");
}

#[test]
fn dispatches_route_tree() {
    check(CONFIG, 8, |peer| {
        let middle = Request::new(b"/tree1")
            .try_start(peer.client, Boundary::End)
            .expect("start middle tree route");
        let last = Request::new(b"/tree7")
            .try_start(peer.client, Boundary::End)
            .expect("start last tree route");
        peer.try_flush().expect("H2 peer flush");

        let (_, bodies) = peer
            .try_statuses(&[middle, last], b"200")
            .expect("H2 peer statuses");
        let bodies = <[Vec<u8>; 2]>::try_from(bodies).expect("two response bodies");
        assert_eq!(bodies[0], b"quick");
        assert_eq!(bodies[1], b"quick");
    })
    .expect("H2 thirteen-route runtime");
}

#[test]
fn retains_indexed_header() {
    check(CONFIG, 8, |peer| {
        let retained = peer
            .client
            .streams()
            .headers()
            .request(
                Request::new(b"/users/beta/teams/alpha/retained-header?q=request-query")
                    .try_named(RETAINED_NAME)
                    .expect("valid named request fields"),
                Boundary::End,
            )
            .expect("start retained-header request");
        peer.socket.set_nodelay(true).expect("H2 TCP_NODELAY");
        peer.try_fragment(NonZero::new(32).expect("fragment size"))
            .expect("H2 peer fragment");
        for index in 0..40 {
            let owner = format!("turnover-{index:03}-{}", "x".repeat(96));
            peer.client
                .streams()
                .headers()
                .request(
                    Request::new(b"/quick")
                        .try_named(owner.as_bytes())
                        .expect("valid request fields"),
                    Boundary::End,
                )
                .expect("start table-turnover request");
        }
        peer.try_flush().expect("H2 peer flush");

        let (headers, response) = peer.try_response(retained).expect("H2 peer response");
        assert_eq!(
            headers
                .iter()
                .find(|field| field.name == b":status")
                .map(|field| field.value.as_slice()),
            Some(&b"200"[..]),
        );
        assert_eq!(response, RETAINED_NAME);
    })
    .expect("H2 retained indexed header runtime");
}

#[test]
fn projects_segmented_owner() {
    check(CONFIG, 8, |peer| {
        let stream = Request::new(b"/segmented")
            .try_start(peer.client, Boundary::More)
            .expect("start H2 request");
        try_send(peer.client, stream, b"first-", Boundary::More).expect("H2 peer send");
        peer.try_flush().expect("H2 peer flush");
        try_send(peer.client, stream, b"second", Boundary::End).expect("H2 peer send");
        peer.try_flush().expect("H2 peer flush");

        let (headers, response) = peer.try_response(stream).expect("H2 peer response");
        assert_eq!(
            headers
                .iter()
                .find(|field| field.name == b":status")
                .map(|field| field.value.as_slice()),
            Some(&b"200"[..]),
        );
        assert_eq!(response, b"first-second");
    })
    .expect("H2 segmented sync runtime");
}

#[test]
fn trailers_dispatch_once() {
    check(CONFIG, 8, |peer| {
        let stream = Request::new(b"/nested")
            .try_start(peer.client, Boundary::More)
            .expect("start trailer-terminated request");
        let body = NESTED_JSON;
        try_send(peer.client, stream, body, Boundary::More).expect("H2 peer send");
        peer.client
            .streams()
            .headers()
            .trail(
                stream,
                [const {
                    Entry::new(
                        field::literal!(Lower::try_bytes(b"x-checksum")),
                        field::literal!(Value::try_bytes(b"ok")),
                    )
                }],
            )
            .expect("send request trailers");
        peer.try_flush().expect("H2 peer flush");

        let (_, response) = peer.try_response(stream).expect("H2 peer response");
        assert_eq!(response, b"retained");
    })
    .expect("H2 trailer task runtime");
}

#[test]
fn guard_precedes_body() {
    let harness = Pair::bind().expect("H2 harness");
    let mut config = CONFIG;
    config.max_request_body_bytes = 1;
    config.max_connection_body_bytes = 1;

    harness
        .check(
            move |context, listener| {
                let server = Server::new(config, 1, State::with_guard(GuardProbe::default()));
                let result = server.try_run(context, listener, mode::Disabled);
                result.map(|()| server.state.guard_counts())
            },
            |address| {
                let (mut socket, mut client) = Peer::connect(address);
                let mut peer = Peer::new(&mut socket, &mut client);
                let stream = Request::new(b"/guarded/body")
                    .try_start(peer.client, Boundary::More)
                    .expect("start guarded H2 request");
                try_send(peer.client, stream, b"not admitted", Boundary::More)
                    .expect("H2 peer send");
                peer.try_flush().expect("H2 peer flush");

                let (headers, body) = peer.try_response(stream).expect("H2 peer response");
                assert!(
                    headers.iter().any(|field| {
                        field.name == b":status" && field.value.as_slice() == b"401"
                    })
                );
                assert_eq!(body, b"guarded");
            },
            |result| {
                assert_eq!(result.expect("H2 setup"), (1, 0));
            },
        )
        .expect("guarded H2 runtime");
}
