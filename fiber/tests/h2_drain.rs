pub mod common;

use std::{cell::Cell, io::Read as _, time::Duration};

use common::h2::recover;
use common::{
    h2::{
        events::{Log, PoolLog},
        facts::State,
        peer::Peer,
        server::{CONFIG, Request, Server},
    },
    route::keep::events::diagnose,
};
use dope::manifold::network::listener::handler;
use dope_test::harness::server::Pair;
use o3::{
    base::time::Positive,
    buffer::{hold, pool},
};
use sark_core::{
    deadline::{Phase, config},
    life::mode,
    manifold::stream::Boundary,
};
use sark_h2::{
    conn::event::Event,
    frame::numbers::{ErrorCode, Increment},
    server,
};

#[test]
fn sent_completes_drain() {
    Pair::bind()
        .expect("server")
        .check(
            |context, listener| {
                let config = server::Config {
                    max_outbound_bytes: 1,
                    ..CONFIG
                };
                Server::new(config, 8, State::try_body().expect("large response owner"))
                    .with_life(handler::Lifetime::graceful(
                        Positive::from_millis::<250>(),
                        Positive::from_secs::<10>(),
                    ))
                    .try_run(context, listener, mode::Disabled, recover, |result| {
                        result.expect("H2 execution")
                    })
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                Peer::new(&mut socket, &mut client)
                    .try_settings()
                    .expect("H2 settings");
                Peer::new(&mut socket, &mut client)
                    .try_pause()
                    .expect("pause response window");
                let stream = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/unbounded")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("request");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                let mut goaway = false;
                let mut credited = false;
                let mut ended = false;
                let mut body = 0;
                let mut input = [0; 16 << 10];
                loop {
                    let received = socket.read(&mut input).expect("EOF before drain timeout");
                    if received == 0 {
                        break;
                    }
                    client
                        .ingress()
                        .ingest(&input[..received])
                        .expect("H2 input");
                    while let Some(event) = client.ingress().poll_event() {
                        match event {
                            Event::GoAway {
                                last_stream_id: last,
                                error,
                                ..
                            } => {
                                assert_eq!(last, stream);
                                assert_eq!(error, ErrorCode::NoError);
                                goaway = true;
                            }
                            Event::Headers {
                                stream_id: id,
                                boundary,
                                ..
                            } => {
                                assert_eq!(id, stream);
                                assert!(matches!(boundary, Boundary::More));
                            }
                            Event::Data {
                                stream_id: id,
                                data,
                                boundary,
                            } => {
                                assert!(credited);
                                assert_eq!(id, stream);
                                data.for_each(|segment| {
                                    assert!(segment.iter().all(|byte| *byte == 0x6b));
                                    body += segment.len();
                                });
                                ended |= matches!(boundary, Boundary::End);
                            }
                            event => {
                                assert!(matches!(event, Event::SettingsAck), "unexpected H2 event",)
                            }
                        }
                    }
                    if goaway && !credited {
                        Peer::new(&mut socket, &mut client)
                            .try_credit(stream, Increment::new(2 << 20).expect("body credit"))
                            .expect("release response window");
                        credited = true;
                    }
                }
                assert!(goaway && ended);
                assert_eq!(body, 2 << 20);
            },
            |result| result.expect("H2 setup"),
        )
        .expect("response drain runtime");
}

#[test]
fn deadline_completes_drain() {
    for blocked in [false, true] {
        let timer =
            config::Config::try_new(Phase::Total, Duration::from_millis(500)).expect("deadline");
        Pair::bind()
            .expect("server")
            .check(
                move |context, listener| {
                    let state = if blocked {
                        State::try_body().expect("large response owner")
                    } else {
                        State::default()
                    };
                    Server::new(CONFIG, 8, state)
                        .with_life(handler::Lifetime::graceful(
                            Positive::from_millis::<250>(),
                            Positive::from_secs::<10>(),
                        ))
                        .try_run(context, listener, timer, recover, |result| {
                            result.expect("H2 execution")
                        })
                },
                |address| {
                    let (mut socket, mut client) =
                        Peer::try_connect(address, Duration::from_secs(3))
                            .expect("H2 peer connection");
                    Peer::new(&mut socket, &mut client)
                        .try_settings()
                        .expect("H2 settings");
                    if blocked {
                        Peer::new(&mut socket, &mut client)
                            .try_pause()
                            .expect("pause response window");
                    }
                    let stream = client
                        .streams()
                        .headers()
                        .request(
                            Request::new(if blocked { b"/unbounded" } else { b"/nested" })
                                .try_headers()
                                .expect("valid request fields"),
                            if blocked {
                                Boundary::End
                            } else {
                                Boundary::More
                            },
                        )
                        .expect("request");
                    Peer::new(&mut socket, &mut client)
                        .try_flush()
                        .expect("H2 peer flush");
                    let mut goaway = false;
                    let mut reset = false;
                    let mut committed = false;
                    let mut input = [0; 4096];
                    loop {
                        let received = socket.read(&mut input).expect("EOF before drain timeout");
                        if received == 0 {
                            break;
                        }
                        client
                            .ingress()
                            .ingest(&input[..received])
                            .expect("H2 input");
                        while let Some(event) = client.ingress().poll_event() {
                            match event {
                                Event::GoAway {
                                    last_stream_id: last,
                                    error,
                                    ..
                                } => {
                                    assert_eq!(last, stream);
                                    assert_eq!(error, ErrorCode::NoError);
                                    goaway = true;
                                }
                                Event::StreamReset {
                                    stream_id: id,
                                    error,
                                    ..
                                } => {
                                    assert!(goaway);
                                    assert_eq!(id, stream);
                                    assert_eq!(error, ErrorCode::Cancel);
                                    reset = true;
                                }
                                Event::Headers {
                                    stream_id,
                                    boundary,
                                    ..
                                } => {
                                    assert!(blocked);
                                    assert_eq!(stream_id, stream);
                                    assert!(matches!(boundary, Boundary::More));
                                    committed = true;
                                }
                                event => {
                                    assert!(
                                        matches!(event, Event::SettingsAck),
                                        "unexpected H2 event",
                                    )
                                }
                            }
                        }
                    }
                    assert!(goaway && reset);
                    assert_eq!(committed, blocked);
                },
                |result| result.expect("H2 setup"),
            )
            .expect("deadline runtime");
    }
}

#[test]
fn releases_before_notify() {
    Pair::bind()
        .expect("H2 harness")
        .check(
            |context, listener| {
                let pool = pool::Pool::try_new(1, 16).expect("one response slot");
                let body = pool
                    .try_copy(b"retained")
                    .expect("response fits")
                    .expect("free slot")
                    .into_shared();
                let state = State::default();
                state.body.set(Some(body));
                let log = Cell::new(Vec::new());
                let released = Cell::new(None);
                Server::new(CONFIG, 1, state)
                    .try_run(
                        context,
                        listener,
                        mode::Observed::new(PoolLog {
                            log: Log(&log, diagnose),
                            pool: &pool,
                            released: &released,
                        }),
                        recover,
                        |result| result.expect("H2 execution"),
                    )
                    .map(|()| (log.into_inner(), released.get()))
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                Peer::new(&mut socket, &mut client)
                    .try_settings()
                    .expect("H2 settings");
                Peer::new(&mut socket, &mut client)
                    .try_pause()
                    .expect("block response DATA");
                let stream = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/unbounded")
                            .try_headers()
                            .expect("request fields"),
                        Boundary::End,
                    )
                    .expect("response stream");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("request flush");
                let mut input = [0; 4096];
                loop {
                    let read = socket.read(&mut input).expect("response HEADERS");
                    assert_ne!(read, 0, "response must commit before closing");
                    client
                        .ingress()
                        .ingest(&input[..read])
                        .expect("response input");
                    while let Some(event) = client.ingress().poll_event() {
                        if matches!(event, Event::Headers {
                            stream_id, boundary: Boundary::More, ..
                        } if stream_id == stream)
                        {
                            return;
                        }
                    }
                    Peer::new(&mut socket, &mut client)
                        .try_flush()
                        .expect("settings acknowledgement");
                }
            },
            |result| {
                let (facts, released) = result.expect("H2 setup");
                assert_eq!(released, Some((hold::Charge::from_owner(8, 16), 1)));
                let events = facts
                    .iter()
                    .filter_map(|&(kind, _)| {
                        matches!(
                            kind,
                            "response-committed"
                                | "response-staging-released"
                                | "terminal"
                                | "exchange-retired"
                        )
                        .then_some(kind)
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    events,
                    [
                        "response-committed",
                        "response-staging-released",
                        "terminal",
                        "exchange-retired"
                    ]
                );
            },
        )
        .expect("observed H2 body retirement");
}
