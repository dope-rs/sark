use std::{
    cell::Cell,
    io::{Read as _, Write as _},
    net::{Shutdown, TcpStream},
    os::fd::AsFd as _,
    time::Duration,
};

use dope::{
    core::{
        Backend,
        dope_sys::io::socket::option::{Kind, Source as _, Value},
        driver::{life::recovery, route::KeyTag},
        io::socket::{Listener, option},
    },
    manifold::{app::env::Bundle, network::listener::topology::Direct, timing::Throughput},
    net::{tcp, wire},
    runtime::{executor::session::Completion, topology},
};
use dope_test::{
    alloc::Tracker,
    fibers,
    harness::{GUARD, scenario::process::Deadline, signals::LocalGate},
};
use o3::buffer::storage;
use sark_core::duplex::Session as _;
use sark_http::h1::head::{Boundary, ParseError};
use sark_ws::{
    inbound,
    server::{self, app::App},
};

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
fn reports_fault_before_deferred_close() {
    let fault = inbound::Fault::Mask {
        expected: inbound::Mask::Masked,
        actual: inbound::Mask::Clear,
    };
    for retry in [false, true] {
        let calls = Cell::new(0);
        let failure = Cell::new(None);
        let sink = Echo {
            calls: &calls,
            repeat: None,
            activity: &|_| {},
            failed: &|error| {
                let server::Error::Frame(error) = error else {
                    panic!("original frame fault: {error:?}");
                };
                assert_eq!(failure.replace(Some(error)), None);
            },
        };
        let mut session = server::Session::new(sink, 1024, 1024);
        let input = storage::Shared::try_copy(b"\x81\x00").expect("frame owner");
        let mut scratch = Vec::new();
        assert_eq!(
            session.receive(input.into(), &mut [0; 3], &mut scratch).0,
            0
        );
        assert_eq!(failure.get(), Some(fault));
        if retry {
            let mut output = [0; 4];
            assert_eq!(session.activate(&mut output, &mut scratch).0, output.len());
            assert_eq!(output, [0x88, 0x02, 0x03, 0xea]);
        }
        drop(session);
    }
}

#[test]
fn fault_survives_eof() {
    let fault = inbound::Fault::Mask {
        expected: inbound::Mask::Masked,
        actual: inbound::Mask::Clear,
    };
    for (tail, expected) in [(b"\x81\x00".as_slice(), Some(fault)), (b"\x81", None)] {
        Deadline::with(Duration::from_secs(30), |_| {
            let bound = Listener::bind(([127, 0, 0, 1], 0).into()).expect("listener");
            let bind = bound.local_addr();
            let calls = Cell::new(0);
            let failure = Cell::new(None);
            let payload = vec![0xa7; 4 * 1024 * 1024];
            let sink = Echo {
                calls: &calls,
                repeat: Some(&payload),
                activity: &|_| {},
                failed: &|error| {
                    let error = match error {
                        server::Error::Frame(error) => Ok(error),
                        error => Err(error),
                    }
                    .expect("original frame fault");
                    assert_eq!(failure.replace(Some(error)), None);
                },
            };
            let config = eof::config(
                bind,
                1,
                option::Buffer::try_from(1024_usize).expect("buffer"),
            )
            .expect("listener configuration");
            let listener =
                Direct::<KeyTag<0>, Bundle<tcp::Tcp, wire::Identity, Throughput>, _>::new(
                    config,
                    App::new(sink, "/".into(), 16 * 1024 * 1024, 16 * 1024 * 1024),
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
                                let (root, wait) = root.into_parts();
                                let result =
                                    stage.with_quiesce(completion, root, GUARD, |mut app| {
                                        let mut socket =
                                            TcpStream::try_socket(bind).expect("connection");
                                        socket
                                            .set_write_timeout(Some(GUARD))
                                            .expect("input deadline");
                                        socket.write_all(OPENING).expect("opening");
                                        socket.set_nonblocking(true).expect("nonblocking peer");
                                        let mut buffered = Vec::new();
                                        buffered = peer::try_poll(&mut app, &mut || {
                                            socket.try_upgrade(&mut buffered)
                                        })
                                        .expect("upgrade");
                                        let ping = [0x5a; 125];
                                        let mut batch =
                                            b"start".try_masked(0x2, true).expect("request");
                                        for _ in 0..129 {
                                            batch.extend(ping.try_masked(0x9, true).expect("ping"));
                                        }
                                        batch.extend_from_slice(tail);
                                        socket.set_nonblocking(false).expect("bounded batch");
                                        socket.write_all(&batch).expect("request and controls");
                                        socket.shutdown(Shutdown::Write).expect("peer EOF");
                                        socket.set_nonblocking(true).expect("nonblocking peer");
                                        assert_eq!(
                                            peer::try_poll(&mut app, &mut || {
                                                socket.try_next(&mut buffered)
                                            })
                                            .expect("large response"),
                                            (0x2, vec![0xa7; 4 * 1024 * 1024])
                                        );
                                        for _ in 0..129 {
                                            assert_eq!(
                                                peer::try_poll(&mut app, &mut || {
                                                    socket.try_next(&mut buffered)
                                                })
                                                .expect("ordered pong"),
                                                (0xa, ping.to_vec())
                                            );
                                        }
                                        if expected.is_some() {
                                            assert_eq!(
                                                peer::try_poll(&mut app, &mut || {
                                                    socket.try_next(&mut buffered)
                                                })
                                                .expect("protocol close"),
                                                (0x8, 1002u16.to_be_bytes().to_vec())
                                            );
                                        }
                                        assert!(buffered.is_empty());
                                        assert_eq!(
                                            peer::try_poll(&mut app, &mut || {
                                                socket.read(&mut [0; 1]).map_err(peer::Read::Io)
                                            })
                                            .expect("server EOF"),
                                            0
                                        );
                                    });
                                assert!(wait.watch().error().is_none(), "listener accept");
                                result
                            })
                        })
                    },
                    recovery::Policy::new(|_| {}, common::recover),
                    |result| assert!(matches!(result, Ok(Ok(Ok(()))))),
                );
            assert_eq!(calls.get(), 1);
            assert_eq!(failure.get(), expected);
        })
        .expect("server execution");
    }
}

#[test]
fn overflow_survives_eof() {
    for upgraded in [true, false] {
        let length = if upgraded {
            16 * 1024 * 1024 + 64 * 1024 + 1
        } else {
            16 * 1024 + 1
        };
        Deadline::with(Duration::from_secs(30), |_| {
            let bound = Listener::bind(([127, 0, 0, 1], 0).into()).expect("listener");
            let bind = bound.local_addr();
            let calls = Cell::new(0);
            let failure = Cell::new(None);
            let observed = LocalGate::new();
            let ended = || observed.handle().hit();
            let payload = vec![0xa7; 4 * 1024 * 1024];
            let sink = Echo {
                calls: &calls,
                repeat: Some(&payload),
                activity: &|_| {},
                failed: &|error| {
                    assert!(
                        failure.replace(Some(error)).is_none(),
                        "failure delivered once"
                    );
                    ended();
                },
            };
            let app = eof::Observed {
                inner: App::new(sink, "/".into(), 16 * 1024 * 1024, 16 * 1024 * 1024),
                timing: None,
                ended: Some(&ended),
                received: None,
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
                                let (root, wait) = root.into_parts();
                                let result =
                                    stage.with_quiesce(completion, root, GUARD, |mut app| {
                                        let held =
                                            TcpStream::try_socket(bind).expect("held connection");
                                        let mut socket =
                                            TcpStream::try_socket(bind).expect("input connection");
                                        let mut response = Vec::new();
                                        let mut buffered = Vec::new();
                                        for (mut stream, pending, upgrade) in [
                                            (&held, &mut response, true),
                                            (&socket, &mut buffered, upgraded),
                                        ] {
                                            if upgrade {
                                                stream
                                                    .set_write_timeout(Some(GUARD))
                                                    .expect("input deadline");
                                                stream.write_all(OPENING).expect("opening");
                                            }
                                            stream.set_nonblocking(true).expect("nonblocking peer");
                                            if upgrade {
                                                *pending = peer::try_poll(&mut app, &mut || {
                                                    stream.try_upgrade(pending)
                                                })
                                                .expect("upgrade");
                                            }
                                        }
                                        Backend::set(
                                            held.as_fd(),
                                            Kind::RecvBuffer,
                                            &Value::new(256 * 1024),
                                        )
                                        .expect("bounded receive window");
                                        assert!(response.is_empty());
                                        peer::try_write(
                                            &mut app,
                                            &held,
                                            &b"start".try_masked(0x2, true).expect("request"),
                                        )
                                        .expect("start held response");
                                        let mut header = (&held).take(10);
                                        peer::try_poll(&mut app, &mut || {
                                            header
                                                .read_to_end(&mut response)
                                                .map_err(peer::Read::Io)
                                        })
                                        .expect("held frame header");
                                        assert_eq!(response, [0x82, 127, 0, 0, 0, 0, 0, 64, 0, 0]);

                                        let input = if upgraded {
                                            vec![0; length]
                                        } else {
                                            let mut input = try_padded(5).expect("padded opening");
                                            input.truncate(length);
                                            input
                                        };
                                        peer::try_write(&mut app, &socket, &input)
                                            .expect("overflow the blocked accumulator");
                                        let end = socket.shutdown(Shutdown::Write);
                                        if upgraded {
                                            end.expect("input EOF");
                                        }
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(&mut app, &observed.handle(), 1)
                                                .expect("input settled before releasing output"),
                                            fibers::Wait::Reached
                                        );

                                        assert_eq!(
                                            peer::try_poll(&mut app, &mut || held
                                                .try_next(&mut response))
                                            .expect("held response"),
                                            (0x2, vec![0xa7; 4 * 1024 * 1024])
                                        );
                                        if upgraded {
                                            assert_eq!(
                                                peer::try_poll(&mut app, &mut || {
                                                    socket.try_next(&mut buffered)
                                                })
                                                .expect("message-too-big close"),
                                                (0x8, 1009u16.to_be_bytes().to_vec())
                                            );
                                        } else {
                                            let (head, tail) =
                                                peer::try_poll(&mut app, &mut || {
                                                    socket
                                                        .try_head(&mut buffered)
                                                        .map_err(peer::Read::Io)
                                                })
                                                .expect("deferred opening rejection");
                                            assert!(
                                                head.starts_with(b"HTTP/1.1 400 Bad Request\r\n")
                                            );
                                            assert!(tail.is_empty());
                                        }
                                        assert!(buffered.is_empty());
                                        assert_eq!(
                                            peer::try_poll(&mut app, &mut || {
                                                socket.read(&mut [0; 1]).map_err(peer::Read::Io)
                                            })
                                            .expect("server EOF"),
                                            0
                                        );
                                    });
                                assert!(wait.watch().error().is_none(), "listener accept");
                                result
                            })
                        })
                    },
                    recovery::Policy::new(|_| {}, common::recover),
                    |result| assert!(matches!(result, Ok(Ok(Ok(()))))),
                );
            assert_eq!(calls.get(), 1);
            let failure = failure.take();
            if upgraded {
                let error = match failure {
                    Some(server::Error::Capacity(error)) => error,
                    error => panic!("exact accumulator overflow: {error:?}"),
                };
                let (required, available) = error.into_parts();
                assert_eq!(required.get(), available + 1);
            } else {
                assert!(
                    matches!(
                        &failure,
                        Some(server::Error::Open(ParseError::Boundary(Boundary::TooLarge {
                            actual,
                            limit,
                        }))) if *actual == length && *limit == 16 * 1024
                    ),
                    "exact opening overflow: {failure:?}"
                );
            }
        })
        .expect("server execution");
    }
}
