use std::{
    cell::Cell,
    convert::Infallible,
    io::{ErrorKind, Read as _, Write as _},
    net::{Shutdown, TcpStream},
    os::fd::AsFd as _,
    time::{Duration, Instant},
};

use dope::{
    core::{
        Backend,
        dope_sys::io::socket::option::{Kind, Source as _, Value},
        driver::{life::recovery, route::KeyTag},
        io::socket::option,
        platform::wake,
    },
    manifold::{
        app::env::Bundle,
        network::listener::{handler, topology::Direct},
        timing::{Policy, Throughput},
    },
    net::{tcp, wire},
    runtime::{executor::session::Completion, topology, topology::ops::Run},
};
use dope_test::{alloc::Tracker, harness::server::Pair};
use sark_http::h1::head::{Boundary, ParseError};
use sark_ws::server::{self, app::App};

pub mod common;

use common::{
    echo::{Echo, OPENING},
    peer::Peer as _,
    protocol::Payload as _,
    server::eof,
    upgrade::server::try_padded,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn opening_deadline_is_absolute_and_ends_after_admission() {
    Pair::bind()
        .expect("listener")
        .run(
            |context, bound| -> Result<(), Infallible> {
                let calls = Cell::new(0);
                let failures = Cell::new(0);
                let payload = vec![0xa7; 4 * 1024 * 1024];
                let sink = Echo {
                    calls: &calls,
                    repeat: Some(&payload),
                    activity: &|_| {},
                    failed: &|error| {
                        assert!(matches!(error, server::Error::OpenTimeout), "{error:?}");
                        failures.set(failures.get() + 1);
                    },
                };
                let app = eof::Observed {
                    inner: App::new(sink, "/".into(), 16 * 1024 * 1024, 16 * 1024 * 1024),
                    timing: Some(handler::Timing::timeout(Duration::from_millis(250))),
                    ended: None,
                    received: None,
                };
                let opening = handler::Application::<KeyTag<0>>::timing(&app.inner);
                assert!(
                    opening > handler::Timing::timeout(Duration::ZERO)
                        && opening < handler::Timing::timeout(Throughput::IDLE_WINDOW.get()),
                    "opening timeout must precede established idle timeout"
                );
                let config = eof::config(
                    bound.local_addr(),
                    4,
                    option::Buffer::try_from(256 * 1024_usize).expect("buffer"),
                )
                .expect("listener configuration");
                let listener =
                    Direct::<KeyTag<0>, Bundle<tcp::Tcp, wire::Identity, Throughput>, _>::new(
                        config, app,
                    )
                    .with_bound(Some(bound));
                topology::Plan::new((), listener)
                    .for_install::<Throughput>()
                    .expect("listener layout")
                    .enter(
                        context,
                        Run(&mut Completion::new()),
                        recovery::Policy::new(|_| {}, common::recover),
                        |result| assert!(matches!(result, Ok(Ok(_)))),
                    );
                assert_eq!(failures.get(), 3);
                assert_eq!(calls.get(), 1);
                Ok(())
            },
            |bind| {
                let (mut active, mut response) = TcpStream::try_connect(bind).expect("upgrade");
                Backend::set(active.as_fd(), Kind::RecvBuffer, &Value::new(256 * 1024))
                    .expect("bounded receive window");
                active
                    .write_all(&b"live".try_masked(0x2, true).expect("frame"))
                    .expect("hold admitted response");
                response.resize(10, 0);
                active.read_exact(&mut response).expect("held frame header");
                assert_eq!(response, [0x82, 127, 0, 0, 0, 0, 0, 64, 0, 0]);
                let mut silent = TcpStream::try_socket(bind).expect("silent opening");
                let mut slow = TcpStream::try_socket(bind).expect("slow opening");
                let mut blocked = TcpStream::try_socket(bind).expect("blocked opening");
                blocked
                    .write_all(OPENING)
                    .expect("complete blocked opening");
                slow.set_read_timeout(Some(Duration::from_millis(20)))
                    .expect("dribble interval");
                slow.write_all(b"GET / HTTP/1.1\r\nHost: ")
                    .expect("incomplete head");
                let started = Instant::now();
                loop {
                    assert!(
                        started.elapsed() < Duration::from_secs(5),
                        "absolute deadline"
                    );
                    match slow.read(&mut [0; 1]) {
                        Ok(0) => break,
                        Ok(_) => panic!("incomplete opening has no response"),
                        Err(error)
                            if matches!(
                                error.kind(),
                                ErrorKind::WouldBlock | ErrorKind::TimedOut
                            ) =>
                        {
                            match slow.write_all(b"x") {
                                Ok(()) => {}
                                Err(error)
                                    if matches!(
                                        error.kind(),
                                        ErrorKind::BrokenPipe | ErrorKind::ConnectionReset
                                    ) =>
                                {
                                    break;
                                }
                                Err(error) => panic!("opening write: {error}"),
                            }
                        }
                        Err(error) if error.kind() == ErrorKind::ConnectionReset => break,
                        Err(error) => panic!("opening read: {error}"),
                    }
                }
                assert_eq!(silent.read(&mut [0; 1]).expect("silent opening EOF"), 0);
                assert_eq!(blocked.read(&mut [0; 1]).expect("blocked opening EOF"), 0);
                assert_eq!(
                    active
                        .try_next(&mut response)
                        .expect("admitted connection outlives opening deadline"),
                    (0x2, vec![0xa7; 4 * 1024 * 1024])
                );
            },
        )
        .expect("server execution");
}

#[test]
fn opening_bound_precedes_retention() {
    const LIMIT: usize = 16 * 1024;
    for (length, split) in [(LIMIT, 1), (LIMIT, 2), (LIMIT, 3), (LIMIT + 1, 1)] {
        let accepted = length == LIMIT;
        let start = b"start".try_masked(0x2, true).expect("held request");
        let prefix_end = OPENING.len() + start.len() + length - split;
        let (prefix_wait, prefix_seen) = wake::Ends::blocking().expect("prefix barrier").split();
        let (settled, ended) = wake::Ends::blocking().expect("input barrier").split();
        Pair::bind()
            .expect("listener")
            .run(
                move |context, bound| -> Result<(), Infallible> {
                    let calls = Cell::new(0);
                    let rejected = Cell::new(false);
                    let received = Cell::new(0);
                    let notified = Cell::new(false);
                    let ended = || {
                        if !notified.replace(true) {
                            ended.notify().expect("settled input");
                        }
                    };
                    let payload = vec![0xa7; 4 * 1024 * 1024];
                    let sink = Echo {
                        calls: &calls,
                        repeat: Some(&payload),
                        activity: &|_| {},
                        failed: &|error| {
                            assert!(!accepted, "valid bounded opening: {error:?}");
                            assert!(!rejected.replace(true), "failure delivered once");
                            assert!(
                                matches!(
                                    &error,
                                    server::Error::Open(ParseError::Boundary(Boundary::TooLarge {
                                        actual,
                                        limit,
                                    })) if *actual == LIMIT + 1 && *limit == LIMIT
                                ),
                                "exact opening bound: {error:?}"
                            );
                            ended();
                        },
                    };
                    let observed = |length, allocations| {
                        received.set(received.get() + length);
                        if received.get() == prefix_end {
                            prefix_seen.notify().expect("retained incomplete prefix");
                        }
                        if rejected.get() {
                            assert_eq!(allocations, (0, 0), "reject before retaining input");
                        }
                    };
                    let app = eof::Observed {
                        inner: App::new(sink, "/".into(), 16 * 1024 * 1024, 16 * 1024 * 1024),
                        timing: None,
                        ended: Some(&ended),
                        received: Some(&observed),
                    };
                    let config = eof::config(
                        bound.local_addr(),
                        2,
                        option::Buffer::try_from(256 * 1024_usize).expect("buffer"),
                    )
                    .expect("listener configuration");
                    let listener =
                        Direct::<KeyTag<0>, Bundle<tcp::Tcp, wire::Identity, Throughput>, _>::new(
                            config, app,
                        )
                        .with_bound(Some(bound));
                    topology::Plan::new((), listener)
                        .for_install::<Throughput>()
                        .expect("listener layout")
                        .enter(
                            context,
                            Run(&mut Completion::new()),
                            recovery::Policy::new(|_| {}, common::recover),
                            |result| assert!(matches!(result, Ok(Ok(_)))),
                        );
                    assert_eq!(calls.get(), 1 + usize::from(accepted));
                    assert_eq!(rejected.get(), !accepted);
                    Ok(())
                },
                |bind| {
                    let (mut held, mut response) =
                        TcpStream::try_connect(bind).expect("held upgrade");
                    let mut socket = TcpStream::try_socket(bind).expect("opening connection");
                    Backend::set(held.as_fd(), Kind::RecvBuffer, &Value::new(256 * 1024))
                        .expect("bounded receive window");
                    held.write_all(&start).expect("start held response");
                    response.resize(10, 0);
                    held.read_exact(&mut response).expect("held frame header");
                    assert_eq!(response, [0x82, 127, 0, 0, 0, 0, 0, 64, 0, 0]);

                    let mut request = try_padded(u8::from(!accepted)).expect("padded opening");
                    if accepted {
                        request.extend(
                            vec![0x5a; 32 * 1024]
                                .try_masked(0x2, true)
                                .expect("coalesced large frame"),
                        );
                    }
                    socket
                        .write_all(&request[..length - split])
                        .expect("incomplete opening prefix");
                    prefix_wait
                        .wait_timeout(Duration::from_secs(5))
                        .expect("prefix consumed before delimiter suffix");
                    socket
                        .write_all(&request[length - split..])
                        .expect("delimiter suffix and coalesced frame");
                    let end = socket.shutdown(Shutdown::Write);
                    if accepted {
                        end.expect("opening EOF");
                    }
                    settled
                        .wait_timeout(Duration::from_secs(5))
                        .expect("input settled while output remains blocked");
                    if !accepted {
                        assert_eq!(socket.read(&mut [0; 1]).expect("early rejection EOF"), 0);
                    }

                    assert_eq!(
                        held.try_next(&mut response).expect("held response"),
                        (0x2, vec![0xa7; 4 * 1024 * 1024])
                    );
                    if accepted {
                        let (head, mut tail) = socket.try_head().expect("bounded upgrade");
                        assert!(head.starts_with(b"HTTP/1.1 101"));
                        assert_eq!(
                            socket
                                .try_next(&mut tail)
                                .expect("coalesced frame response"),
                            (0x2, vec![0xa7; 4 * 1024 * 1024])
                        );
                        assert!(tail.is_empty());
                        assert_eq!(socket.read(&mut [0; 1]).expect("server EOF"), 0);
                    }
                },
            )
            .expect("server execution");
    }
}
