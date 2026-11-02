pub mod common;

use std::{
    cell::Cell,
    fmt,
    io::{Read as _, Write as _},
    time::Duration,
};

use common::h2::recover;
use common::h2::{
    events::{Credit, CreditLog, Log},
    facts::State,
    peer::{Peer, Sender},
    server::{CONFIG, Request, Server},
};
use dope::core::driver::schedule;
use dope_test::harness::server::Pair;
use sark_core::{life::mode::Observed, manifold::stream::Boundary};
use sark_h2::{
    conn::{self, event::Rejected},
    frame::numbers,
    hpack::decode::limit::Admission,
    stream::Id,
};
use sark_http::field::metadata::block::Validation;

#[test]
fn preserves_preface_cause() {
    let mut causes = Vec::new();
    Pair::bind()
        .expect("H2 harness")
        .check(
            |context, listener| {
                let events = Cell::new(Vec::new());
                let mut causes = Vec::new();
                Server::new(CONFIG, 2, State::default())
                    .try_run(
                        context,
                        listener,
                        Observed::new(Log(&events, |cause: &dyn fmt::Debug| {
                            causes.push(format!("{cause:?}"));
                        })),
                        recover,
                        |result| result.expect("H2 execution"),
                    )
                    .map(|()| causes)
            },
            |address| {
                let mut socket =
                    Peer::try_open(address, Duration::from_secs(3)).expect("H2 peer connection");
                socket
                    .write_all(b"XRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
                    .expect("invalid preface");
                socket.read_to_end(&mut Vec::new()).expect("H2 close");
            },
            |result| causes = result.expect("H2 setup"),
        )
        .expect("H2 diagnostic runtime");
    assert_eq!(
        causes,
        ["BadPreface { offset: 0, expected: 80, actual: 88 }"],
    );
}

#[test]
fn keeps_queued_rejection() {
    let harness = Pair::bind().expect("H2 harness");
    let (mut socket, mut client) =
        Peer::try_connect(harness.addr(), Duration::from_secs(3)).expect("H2 peer connection");
    let stream = client
        .streams()
        .headers()
        .request(
            Request::new(b"/blocked")
                .try_headers()
                .expect("valid request fields"),
            Boundary::More,
        )
        .expect("start H2 request");
    Peer::new(&mut socket, &mut client)
        .try_faults(stream)
        .expect("queue rejected trailers and fatal headers");
    let mut causes = Vec::new();
    harness
        .check(
            |context, listener| {
                let events = Cell::new(Vec::new());
                let mut causes = Vec::new();
                Server::new(CONFIG, 2, State::default())
                    .try_run(
                        context,
                        listener,
                        Observed::new(Log(&events, |cause: &dyn fmt::Debug| {
                            causes.push(format!("{cause:?}"));
                        })),
                        recover,
                        |result| result.expect("H2 execution"),
                    )
                    .map(|()| causes)
            },
            |_| {
                socket.read_to_end(&mut Vec::new()).expect("H2 close");
            },
            |result| causes = result.expect("H2 setup"),
        )
        .expect("H2 diagnostic runtime");
    let mut expected = [
        format!(
            "{:?}",
            Rejected {
                admit: Admission::Admitted,
                sealed: Validation::Malformed,
                finished: Validation::Malformed,
            }
        ),
        format!(
            "{:?}",
            conn::Error::ParseError(numbers::ParseError::Stream {
                kind: numbers::Type::Headers,
                actual: Id::CONNECTION,
            })
        ),
    ];
    causes.sort_unstable();
    expected.sort_unstable();
    assert_eq!(causes, expected);
}

#[test]
fn json_failure_precedes_next_data() {
    let harness = Pair::bind().expect("H2 harness");
    let (mut socket, mut client) =
        Peer::try_connect(harness.addr(), Duration::from_secs(3)).expect("H2 peer connection");
    let stream = client
        .streams()
        .headers()
        .request(
            Request::new(b"/large")
                .try_headers()
                .expect("valid request fields"),
            Boundary::More,
        )
        .expect("start JSON request");
    // More DATA than one admitted turn can consume, all queued before the
    // server starts. The first invalid byte must fail before the next push.
    for index in 0..2 * schedule::LIMIT {
        Sender::new(&mut client)
            .try_send(
                stream,
                b"!",
                if index + 1 == 2 * schedule::LIMIT {
                    Boundary::End
                } else {
                    Boundary::More
                },
            )
            .expect("queue JSON DATA");
    }
    Peer::new(&mut socket, &mut client)
        .try_flush()
        .expect("flush queued JSON DATA");
    harness
        .check(
            |context, listener| {
                let events = Cell::new(Vec::new());
                let credit = Cell::new(Credit::default());
                let mut causes = Vec::new();
                Server::new(CONFIG, 2, State::default())
                    .try_run(
                        context,
                        listener,
                        Observed::new(CreditLog {
                            log: Log(&events, |cause: &dyn fmt::Debug| {
                                causes.push(format!("{cause:?}"));
                            }),
                            state: &credit,
                        }),
                        recover,
                        |result| result.expect("H2 execution"),
                    )
                    .map(|()| (causes, credit.into_inner()))
            },
            |_| {
                assert_eq!(
                    Peer::new(&mut socket, &mut client)
                        .try_reset(stream)
                        .expect("JSON request reset"),
                    numbers::ErrorCode::ProtocolError,
                );
                let followup = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/quick")
                            .try_headers()
                            .expect("valid follow-up fields"),
                        Boundary::End,
                    )
                    .expect("start follow-up request");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("flush follow-up request");
                let (_, bodies) = Peer::new(&mut socket, &mut client)
                    .try_statuses(&[followup], b"200")
                    .expect("follow-up response after JSON reset");
                assert_eq!(bodies, [b"quick".to_vec()]);
            },
            |result| {
                let (causes, credit) = result.expect("H2 setup");
                assert_eq!(causes, ["Build(R0002(Parse(Invalid)))"]);
                assert_eq!(credit.reserved, (1, 1));
                assert_eq!(credit.released, credit.reserved);
                assert_eq!(credit.closing, Some((0, 0)));
                assert!(!credit.invalid);
            },
        )
        .expect("H2 JSON cleanup runtime");
}
