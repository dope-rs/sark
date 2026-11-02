pub mod common;

use std::{cell::Cell, fmt, iter, time::Duration};

use common::h2::recover;
use common::h2::{
    apps::{PHYSICAL_BODY, STATIC_DUPLEX_BODY, STATIC_DUPLEX_INPUT},
    events::Log,
    facts::{Identity, State},
    peer::{Peer, Sender},
    server::{CONFIG, CONNECT, EXTENDED, Request, Server},
};

use dope_test::harness::server::Pair;
use sark_core::{life::mode::Observed, manifold::stream::Boundary};
use sark_h2::{
    ClientRole,
    conn::{
        config::{self, Settings},
        machine::Connection,
    },
};
use sark_http::field::metadata::block::Connect;

#[test]
fn classic_header_owner_fin() {
    prove_h2_duplex(Connect::Disabled, b"h2-duplex-owner", b"h2-duplex-owner", 1);
}

#[test]
fn extended_owner_path() {
    prove_h2_duplex(Connect::Enabled, b"h2-duplex-owner", b"h2-duplex-owner", 1);
}

#[test]
fn static_duplex_bytes() {
    prove_h2_duplex(
        Connect::Disabled,
        STATIC_DUPLEX_INPUT,
        &STATIC_DUPLEX_BODY,
        0,
    );
}

fn prove_h2_duplex(connect: Connect, input: &'static [u8], expected: &'static [u8], stages: usize) {
    let harness = Pair::bind().expect("H2 harness");
    let config = CONFIG;
    let mut facts = Vec::new();

    harness
        .check(
            move |context, listener| {
                let log = Cell::new(Vec::new());
                let server = Server::new(config, 8, State::default());
                let result = server.try_run(
                    context,
                    listener,
                    Observed::new(Log(&log, diagnose)),
                    recover,
                    |result| result.expect("H2 execution"),
                );
                assert_eq!(
                    server.state.header.get(),
                    Some(*b"retained-after-pseudo-fields")
                );
                result.map(|()| log.into_inner())
            },
            |address| {
                let mut socket = Peer::try_open(address, Duration::from_secs(3)).expect("H2 peer");
                let mut client = Connection::<ClientRole>::with_config(config::Config {
                    local_settings: Settings {
                        enable_connect_protocol: connect,
                        ..Settings::DEFAULT
                    },
                    recv_window_target: 65_535,
                    ..Default::default()
                })
                .expect("CONNECT client config");
                Peer::new(&mut socket, &mut client)
                    .try_settings()
                    .expect("H2 peer settings");
                assert!(
                    client.settings().peer().enable_connect_protocol == Connect::Enabled,
                    "the generated extended route must advertise ENABLE_CONNECT_PROTOCOL",
                );
                let stream = match connect {
                    Connect::Enabled => {
                        client.streams().headers().request(EXTENDED, Boundary::More)
                    }
                    Connect::Disabled => {
                        client.streams().headers().request(CONNECT, Boundary::More)
                    }
                }
                .expect("start CONNECT stream");
                Sender::new(&mut client)
                    .try_send(stream, input, Boundary::End)
                    .expect("H2 peer send");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");

                let (headers, body) = Peer::new(&mut socket, &mut client)
                    .try_response(stream)
                    .expect("H2 peer response");
                assert_eq!(
                    headers
                        .iter()
                        .find(|field| field.name == b":status")
                        .map(|field| field.value.as_slice()),
                    Some(&b"200"[..]),
                );
                assert_eq!(body, expected);
            },
            |result| {
                facts = result.expect("H2 setup");
            },
        )
        .expect("CONNECT duplex H2 runtime");

    let stream = facts
        .iter()
        .find_map(|&(kind, stream)| (kind == "exchange-opened").then_some(stream))
        .expect("duplex H2 exchange");
    assert_eq!(
        facts
            .iter()
            .filter(|&&(kind, event_stream)| {
                kind == "response-staging-acquired" && event_stream == stream
            })
            .count(),
        stages,
        "only a real duplex callback owner may enter H2 staging: {facts:?}",
    );
    if stages == 0 {
        assert_eq!(
            facts
                .iter()
                .filter(|&&(kind, event_stream)| {
                    (kind == "response-handoff" || kind == "response-transport-released")
                        && event_stream == stream
                })
                .count(),
            0,
            "a static duplex body must not manufacture a transport owner: {facts:?}",
        );
    }
    assert_eq!(
        facts
            .iter()
            .filter(|&&(kind, event_stream)| kind == "terminal" && event_stream == stream)
            .count(),
        1,
        "the DATA+END_STREAM action must commit one terminal transition: {facts:?}",
    );
}

#[test]
fn duplex_backpressure() {
    let harness = Pair::bind().expect("H2 duplex backpressure harness");
    let mut config = CONFIG;
    config.max_outbound_bytes = 1;
    let mut facts = Vec::new();

    let duplex = harness
        .check(
            move |context, listener| {
                let log = Cell::new(Vec::new());
                Server::new(config, 8, State::default())
                    .try_run(
                        context,
                        listener,
                        Observed::new(Log(&log, diagnose)),
                        recover,
                        |result| result.expect("H2 execution"),
                    )
                    .map(|()| log.into_inner())
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                Peer::new(&mut socket, &mut client)
                    .try_settings()
                    .expect("H2 peer settings");
                let first = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/quick")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("start saturating H2 response");
                let stream = client
                    .streams()
                    .headers()
                    .request(CONNECT, Boundary::More)
                    .expect("start backpressured CONNECT stream");
                Sender::new(&mut client)
                    .try_send(stream, b"first", Boundary::More)
                    .expect("H2 peer send");
                Sender::new(&mut client)
                    .try_send(stream, b"last", Boundary::End)
                    .expect("H2 peer send");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");

                let (_, bodies) = Peer::new(&mut socket, &mut client)
                    .try_statuses(&[first, stream], b"200")
                    .expect("H2 peer statuses");
                assert_eq!(bodies[0], b"quick");
                assert_eq!(bodies[1], b"firstlast");
                u64::from(stream.into_u32())
            },
            |result| {
                facts = result.expect("H2 setup");
            },
        )
        .expect("backpressured CONNECT H2 runtime");

    let events = facts
        .iter()
        .filter_map(|&(kind, stream)| (Identity::from(stream).wire() == duplex).then_some(kind))
        .collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .filter(|&&kind| kind == "input-finished")
            .count(),
        1,
    );
    assert_eq!(events.iter().filter(|&&kind| kind == "terminal").count(), 1,);
    let finished = events
        .iter()
        .position(|&kind| kind == "input-finished")
        .expect("duplex input finish fact");
    let commit = events
        .iter()
        .position(|&kind| kind == "response-committed")
        .expect("duplex response commitment");
    assert!(
        finished < commit,
        "duplex head was not backpressured: {events:?}"
    );
}

#[test]
fn static_burst_no_owner() {
    const BURST: usize = 8;

    let harness = Pair::bind().expect("H2 harness");
    let config = CONFIG;
    let mut facts = Vec::new();

    let streams = harness
        .check(
            move |context, listener| {
                let log = Cell::new(Vec::new());
                Server::new(config, BURST, State::default())
                    .try_run(
                        context,
                        listener,
                        Observed::new(Log(&log, diagnose)),
                        recover,
                        |result| result.expect("H2 execution"),
                    )
                    .map(|()| log.into_inner())
            },
            move |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                let wanted = iter::repeat_with(|| {
                    client
                        .streams()
                        .headers()
                        .request(
                            Request::new(b"/physical")
                                .try_headers()
                                .expect("valid request fields"),
                            Boundary::End,
                        )
                        .expect("start observed H2 burst member")
                })
                .take(BURST)
                .collect::<Vec<_>>();
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                let (_, bodies) = Peer::new(&mut socket, &mut client)
                    .try_statuses(&wanted, b"200")
                    .expect("H2 peer statuses");
                assert!(
                    bodies
                        .iter()
                        .all(|body| body.as_slice() == PHYSICAL_BODY.as_slice())
                );
                wanted
                    .into_iter()
                    .map(|stream| u64::from(stream.into_u32()))
                    .collect::<Vec<_>>()
            },
            |result| {
                facts = result.expect("H2 setup");
            },
        )
        .expect("observed H2 capacity-fill runtime");

    for &stream in streams.iter() {
        assert_eq!(
            facts
                .iter()
                .filter(|&&(kind, event)| {
                    (kind == "response-staging-acquired"
                        || kind == "response-staging-released"
                        || kind == "response-handoff"
                        || kind == "response-transport-released")
                        && Identity::from(event).wire() == stream
                })
                .count(),
            0,
            "wire-visible static bytes must not manufacture a runtime owner: {facts:?}",
        );
    }
}

#[test]
fn large_owner_direct() {
    let harness = Pair::bind().expect("H2 harness");
    let config = CONFIG;
    let mut facts = Vec::new();
    let stream = harness
        .check(
            move |context, listener| {
                let log = Cell::new(Vec::new());
                Server::new(config, 1, State::try_body().expect("large response owner"))
                    .try_run(
                        context,
                        listener,
                        Observed::new(Log(&log, diagnose)),
                        recover,
                        |result| result.expect("H2 execution"),
                    )
                    .map(|()| log.into_inner())
            },
            move |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(5)).expect("H2 peer connection");
                let stream = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/unbounded")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("start unbounded H2 request");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                let (_, bodies) = Peer::new(&mut socket, &mut client)
                    .try_statuses(&[stream], b"200")
                    .expect("H2 peer statuses");
                assert_eq!(bodies[0].len(), 2 * 1024 * 1024);
                assert!(bodies[0].iter().all(|byte| *byte == 0x6b));
                u64::from(stream.into_u32())
            },
            |result| {
                facts = result.expect("H2 setup");
            },
        )
        .expect("unbounded H2 direct-fallback runtime");

    let accepted = facts
        .iter()
        .filter(|&&(kind, event)| {
            kind == "response-handoff" && Identity::from(event).wire() == stream
        })
        .count();
    let released = facts
        .iter()
        .filter(|&&(kind, event)| {
            kind == "response-transport-released" && Identity::from(event).wire() == stream
        })
        .count();
    assert!(
        accepted > 1,
        "the large owner must cross multiple H2 DATA frames"
    );
    assert_eq!(
        accepted, released,
        "every physical owner must settle exactly once"
    );
}

fn diagnose(cause: &dyn fmt::Debug) {
    eprintln!("lifecycle diagnostic: {cause:?}");
}
