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
        io::socket::{Listener, option},
    },
    manifold::{
        app::env::Bundle,
        network::listener::{handler, topology::Direct},
        timing::{Policy, Throughput},
    },
    net::{tcp, wire},
    runtime::{executor::session::Completion, topology, topology::ops::Run},
};
use dope_test::{
    alloc::Tracker,
    fibers,
    harness::{GUARD, scenario::process::Deadline, server::Pair, signals::LocalGate},
};
use sark_http::h1::head::{Boundary, ParseError};
use sark_ws::server::{self, app::App};

pub mod common;

use common::{
    echo::{Echo, OPENING},
    peer::{self, Peer as _},
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
        Deadline::with(Duration::from_secs(30), |_| {
            let accepted = length == LIMIT;
            let start = b"start".try_masked(0x2, true).expect("held request");
            let prefix_end = OPENING.len() + start.len() + length - split;
            let prefix_seen = LocalGate::new();
            let settled = LocalGate::new();
            let bound = Listener::bind(([127, 0, 0, 1], 0).into()).expect("listener");
            let bind = bound.local_addr();
            let calls = Cell::new(0);
            let rejected = Cell::new(false);
            let received = Cell::new(0);
            let ended = || settled.handle().hit();
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
                },
            };
            let observed = |length, allocations| {
                received.set(received.get() + length);
                if received.get() == prefix_end {
                    prefix_seen.handle().hit();
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
                bind,
                2,
                option::Buffer::try_from(256 * 1024_usize).expect("buffer"),
            )
            .expect("listener configuration");
            let listener =
                Direct::<KeyTag<0>, Bundle<tcp::Tcp, wire::Identity, Throughput>, _>::new(
                    config, app,
                )
                .with_bound(Some(bound));
            let completion = &mut Completion::new();
            topology::Plan::new((), listener)
                .for_install::<Throughput>()
                .expect("listener layout")
                .with_local(
                    |worker| {
                        worker.lend(|listener, stage| {
                            listener.install(stage, |root, stage| {
                                stage.with_quiesce(completion, root, GUARD, |mut app| {
                                    let mut held = TcpStream::try_socket(bind).expect("held peer");
                                    held.set_write_timeout(Some(GUARD)).expect("input deadline");
                                    held.write_all(OPENING).expect("held opening");
                                    held.set_nonblocking(true).expect("nonblocking peer");
                                    let mut head = Vec::new();
                                    let mut response = peer::try_poll(&mut app, &mut || {
                                        held.try_upgrade(&mut head)
                                    })
                                    .expect("held upgrade");
                                    let mut socket =
                                        TcpStream::try_socket(bind).expect("opening connection");
                                    socket.set_nonblocking(true).expect("nonblocking opening");
                                    Backend::set(
                                        held.as_fd(),
                                        Kind::RecvBuffer,
                                        &Value::new(256 * 1024),
                                    )
                                    .expect("bounded receive window");
                                    peer::try_write(&mut app, &held, &start)
                                        .expect("start held response");
                                    let mut prefix = (&held).take(10);
                                    peer::try_poll(&mut app, &mut || {
                                        prefix.read_to_end(&mut response).map_err(peer::Read::Io)
                                    })
                                    .expect("held frame header");
                                    assert_eq!(response, [0x82, 127, 0, 0, 0, 0, 0, 64, 0, 0]);

                                    let mut request =
                                        try_padded(u8::from(!accepted)).expect("padded opening");
                                    if accepted {
                                        request.extend(
                                            vec![0x5a; 32 * 1024]
                                                .try_masked(0x2, true)
                                                .expect("coalesced large frame"),
                                        );
                                    }
                                    peer::try_write(&mut app, &socket, &request[..length - split])
                                        .expect("incomplete opening prefix");
                                    assert_eq!(
                                        fibers::TEST
                                            .try_wait(&mut app, &prefix_seen.handle(), 1)
                                            .expect("prefix consumed before delimiter suffix"),
                                        fibers::Wait::Reached
                                    );
                                    peer::try_write(&mut app, &socket, &request[length - split..])
                                        .expect("delimiter suffix and coalesced frame");
                                    let end = socket.shutdown(Shutdown::Write);
                                    if accepted {
                                        end.expect("opening EOF");
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(&mut app, &settled.handle(), 1)
                                                .expect(
                                                    "input settled while output remains blocked"
                                                ),
                                            fibers::Wait::Reached
                                        );
                                    } else {
                                        assert_eq!(
                                            peer::try_poll(&mut app, &mut || {
                                                socket.read(&mut [0; 1]).map_err(peer::Read::Io)
                                            })
                                            .expect("early rejection EOF"),
                                            0
                                        );
                                    }

                                    assert_eq!(
                                        peer::try_poll(&mut app, &mut || held
                                            .try_next(&mut response))
                                        .expect("held response"),
                                        (0x2, vec![0xa7; 4 * 1024 * 1024])
                                    );
                                    if accepted {
                                        let (head, mut tail) =
                                            peer::try_poll(&mut app, &mut || {
                                                socket.try_head(&mut head).map_err(peer::Read::Io)
                                            })
                                            .expect("bounded upgrade");
                                        assert!(head.starts_with(b"HTTP/1.1 101"));
                                        assert_eq!(
                                            peer::try_poll(&mut app, &mut || socket
                                                .try_next(&mut tail))
                                            .expect("coalesced frame response"),
                                            (0x2, vec![0xa7; 4 * 1024 * 1024])
                                        );
                                        assert!(tail.is_empty());
                                        assert_eq!(
                                            peer::try_poll(&mut app, &mut || {
                                                socket.read(&mut [0; 1]).map_err(peer::Read::Io)
                                            })
                                            .expect("server EOF"),
                                            0
                                        );
                                    }
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|_| {}, common::recover),
                    |result| assert!(matches!(result, Ok(Ok(Ok(()))))),
                );
            assert_eq!(calls.get(), 1 + usize::from(accepted));
            assert_eq!(rejected.get(), !accepted);
        })
        .expect("server execution");
    }
}
