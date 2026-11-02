use std::{
    cell::Cell,
    convert::Infallible,
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
        io::socket::option,
        platform::wake,
    },
    manifold::{app::env::Bundle, network::listener::topology::Direct, timing::Throughput},
    net::{tcp, wire},
    runtime::{executor::session::Completion, topology, topology::ops::Run},
};
use dope_test::{alloc::Tracker, harness::server::Pair};
use sark_http::h1::head::{Boundary, ParseError};
use sark_ws::{
    inbound,
    server::{self, app::App},
};

pub mod common;

use common::{
    echo::Echo, peer::Peer as _, protocol::Payload as _, server::eof, upgrade::server::try_padded,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn fault_survives_eof() {
    let fault = inbound::Fault::Mask {
        expected: inbound::Mask::Masked,
        actual: inbound::Mask::Clear,
    };
    for (tail, expected) in [(b"\x81\x00".as_slice(), Some(fault)), (b"\x81", None)] {
        let (wait, sent) = wake::Ends::blocking().expect("peer barrier").split();
        Pair::bind()
            .expect("listener")
            .run(
                move |context, bound| -> Result<(), Infallible> {
                    let calls = Cell::new(0);
                    let failure = Cell::new(None);
                    let payload = vec![0xa7; 4 * 1024 * 1024];
                    let sink = Echo {
                        calls: &calls,
                        repeat: Some(&payload),
                        activity: &|_| {
                            wait.wait_timeout(Duration::from_secs(5))
                                .expect("peer sent EOF before output commit");
                        },
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
                        bound.local_addr(),
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
                    topology::Plan::new((), listener)
                        .for_install::<Throughput>()
                        .expect("listener layout")
                        .enter(
                            context,
                            Run(&mut Completion::new()),
                            recovery::Policy::new(|_| {}, common::recover),
                            |result| assert!(matches!(result, Ok(Ok(_)))),
                        );
                    assert_eq!(calls.get(), 1);
                    assert_eq!(failure.get(), expected);
                    Ok(())
                },
                |bind| {
                    let (mut socket, mut buffered) = TcpStream::try_connect(bind).expect("upgrade");
                    let ping = [0x5a; 125];
                    let mut batch = b"start".try_masked(0x2, true).expect("request");
                    for _ in 0..129 {
                        batch.extend(ping.try_masked(0x9, true).expect("ping"));
                    }
                    batch.extend_from_slice(tail);
                    socket.write_all(&batch).expect("request and controls");
                    socket.shutdown(Shutdown::Write).expect("peer EOF");
                    sent.notify().expect("release response commit");
                    assert_eq!(
                        socket.try_next(&mut buffered).expect("large response"),
                        (0x2, vec![0xa7; 4 * 1024 * 1024])
                    );
                    for _ in 0..129 {
                        assert_eq!(
                            socket.try_next(&mut buffered).expect("ordered pong"),
                            (0xa, ping.to_vec())
                        );
                    }
                    if expected.is_some() {
                        assert_eq!(
                            socket.try_next(&mut buffered).expect("protocol close"),
                            (0x8, 1002u16.to_be_bytes().to_vec())
                        );
                    }
                    assert!(buffered.is_empty());
                    assert_eq!(socket.read(&mut [0; 1]).expect("server EOF"), 0);
                },
            )
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
        let (observed, ended) = wake::Ends::blocking().expect("EOF barrier").split();
        Pair::bind()
            .expect("listener")
            .run(
                move |context, bound| -> Result<(), Infallible> {
                    let calls = Cell::new(0);
                    let failure = Cell::new(None);
                    let notified = Cell::new(false);
                    let ended = || {
                        if !notified.replace(true) {
                            ended.notify().expect("completed input callback");
                        }
                    };
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
                    Ok(())
                },
                |bind| {
                    let (mut held, mut response) =
                        TcpStream::try_connect(bind).expect("held upgrade");
                    let (mut socket, mut buffered) = if upgraded {
                        TcpStream::try_connect(bind).expect("input upgrade")
                    } else {
                        (
                            TcpStream::try_socket(bind).expect("opening connection"),
                            Vec::new(),
                        )
                    };
                    socket
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .expect("input deadline");
                    Backend::set(held.as_fd(), Kind::RecvBuffer, &Value::new(256 * 1024))
                        .expect("bounded receive window");
                    assert!(response.is_empty());
                    held.write_all(&b"start".try_masked(0x2, true).expect("request"))
                        .expect("start held response");
                    response.resize(10, 0);
                    held.read_exact(&mut response).expect("held frame header");
                    assert_eq!(response, [0x82, 127, 0, 0, 0, 0, 0, 64, 0, 0]);

                    let input = if upgraded {
                        vec![0; length]
                    } else {
                        let mut input = try_padded(5).expect("padded opening");
                        input.truncate(length);
                        input
                    };
                    socket
                        .write_all(&input)
                        .expect("overflow the blocked accumulator");
                    let end = socket.shutdown(Shutdown::Write);
                    if upgraded {
                        end.expect("input EOF");
                    }
                    observed
                        .wait_timeout(Duration::from_secs(5))
                        .expect("input settled before releasing output");

                    assert_eq!(
                        held.try_next(&mut response).expect("held response"),
                        (0x2, vec![0xa7; 4 * 1024 * 1024])
                    );
                    if upgraded {
                        assert_eq!(
                            socket
                                .try_next(&mut buffered)
                                .expect("message-too-big close"),
                            (0x8, 1009u16.to_be_bytes().to_vec())
                        );
                    } else {
                        let (head, tail) = socket.try_head().expect("deferred opening rejection");
                        assert!(head.starts_with(b"HTTP/1.1 400 Bad Request\r\n"));
                        assert!(tail.is_empty());
                    }
                    assert!(buffered.is_empty());
                    assert_eq!(socket.read(&mut [0; 1]).expect("server EOF"), 0);
                },
            )
            .expect("server execution");
    }
}
