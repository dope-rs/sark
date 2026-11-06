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
    fibers,
    harness::{GUARD, scenario::process::Deadline, signals::LocalGate},
};
use sark_http::h1::head::ParseError;
use sark_ws::{
    server::{self, app::App},
    upgrade::{Error, KeyError},
};

pub mod common;

use common::{
    echo::{Echo, OPENING},
    peer::{self, Peer as _},
    protocol::Payload as _,
    server::eof,
    upgrade::server::BAD_KEY,
};

#[test]
fn opening_survives_eof() {
    for (request, reject) in [
        (BAD_KEY, true),
        (&BAD_KEY[..BAD_KEY.len() - 2], true),
        (&BAD_KEY[..4], false),
    ] {
        Deadline::with(Duration::from_secs(30), |_| {
            let bound = Listener::bind(([127, 0, 0, 1], 0).into()).expect("listener");
            let bind = bound.local_addr();
            let calls = Cell::new(0);
            let errors = Cell::new(0);
            let failure = Cell::new(None);
            let observed = LocalGate::new();
            let payload = vec![0xa7; 4 * 1024 * 1024];
            let sink = Echo {
                calls: &calls,
                repeat: Some(&payload),
                activity: &|_| {},
                failed: &|error| {
                    errors.set(errors.get() + 1);
                    failure.set(Some(error));
                },
            };
            let ended = || {
                assert_eq!(
                    errors.get(),
                    usize::from(reject),
                    "report rejection once before waiting for output capacity"
                );
                observed.handle().hit();
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
                                    assert!(response.is_empty());
                                    peer::try_write(
                                        &mut app,
                                        &held,
                                        &b"start".try_masked(0x2, true).expect("request"),
                                    )
                                    .expect("start held response");
                                    let mut prefix = (&held).take(10);
                                    peer::try_poll(&mut app, &mut || {
                                        prefix.read_to_end(&mut response).map_err(peer::Read::Io)
                                    })
                                    .expect("held frame header");
                                    assert_eq!(response, [0x82, 127, 0, 0, 0, 0, 0, 64, 0, 0]);

                                    peer::try_write(&mut app, &socket, request)
                                        .expect("opening request");
                                    socket.shutdown(Shutdown::Write).expect("opening EOF");
                                    assert_eq!(
                                        fibers::TEST
                                            .try_wait(&mut app, &observed.handle(), 1)
                                            .expect(
                                                "real app consumed EOF before releasing output"
                                            ),
                                        fibers::Wait::Reached
                                    );

                                    assert_eq!(
                                        peer::try_poll(&mut app, &mut || held
                                            .try_next(&mut response))
                                        .expect("held response"),
                                        (0x2, vec![0xa7; 4 * 1024 * 1024])
                                    );
                                    if reject {
                                        let (head, tail) = peer::try_poll(&mut app, &mut || {
                                            socket.try_head(&mut head).map_err(peer::Read::Io)
                                        })
                                        .expect("deferred rejection");
                                        assert!(head.starts_with(b"HTTP/1.1 400 Bad Request\r\n"));
                                        assert!(tail.is_empty());
                                    }
                                    assert_eq!(
                                        peer::try_poll(&mut app, &mut || {
                                            socket.read(&mut [0; 1]).map_err(peer::Read::Io)
                                        })
                                        .expect("server EOF"),
                                        0
                                    );
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|_| {}, common::recover),
                    |result| assert!(matches!(result, Ok(Ok(Ok(()))))),
                );
            assert_eq!(calls.get(), 1);
            assert_eq!(errors.get(), usize::from(reject));
            let failure = failure.take();
            if reject {
                assert!(
                    matches!(
                        &failure,
                        Some(server::Error::Open(ParseError::Sink(Error::Key(
                            KeyError::Length { actual: 3 }
                        ))))
                    ),
                    "unexpected rejection: {failure:?}"
                );
            } else {
                assert!(failure.is_none(), "unexpected failure: {failure:?}");
            }
        })
        .expect("server execution");
    }
}
