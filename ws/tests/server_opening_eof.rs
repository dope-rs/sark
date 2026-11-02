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
use dope_test::harness::server::Pair;
use sark_http::h1::head::ParseError;
use sark_ws::{
    server::{self, app::App},
    upgrade::{Error, KeyError},
};

pub mod common;

use common::{
    echo::Echo, peer::Peer as _, protocol::Payload as _, server::eof, upgrade::server::BAD_KEY,
};

#[test]
fn opening_survives_eof() {
    for (request, reject) in [
        (BAD_KEY, true),
        (&BAD_KEY[..BAD_KEY.len() - 2], true),
        (&BAD_KEY[..4], false),
    ] {
        let (observed, ended) = wake::Ends::blocking().expect("EOF barrier").split();
        Pair::bind()
            .expect("listener")
            .run(
                move |context, bound| -> Result<(), Infallible> {
                    let calls = Cell::new(0);
                    let errors = Cell::new(0);
                    let failure = Cell::new(None);
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
                        ended.notify().expect("completed EOF callback");
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
                    Ok(())
                },
                |bind| {
                    let (mut held, mut response) =
                        TcpStream::try_connect(bind).expect("held upgrade");
                    let mut socket = TcpStream::try_socket(bind).expect("opening connection");
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

                    socket.write_all(request).expect("opening request");
                    socket.shutdown(Shutdown::Write).expect("opening EOF");
                    observed
                        .wait_timeout(Duration::from_secs(5))
                        .expect("real app consumed EOF before releasing output");

                    assert_eq!(
                        held.try_next(&mut response).expect("held response"),
                        (0x2, vec![0xa7; 4 * 1024 * 1024])
                    );
                    if reject {
                        let (head, tail) = socket.try_head().expect("deferred rejection");
                        assert!(head.starts_with(b"HTTP/1.1 400 Bad Request\r\n"));
                        assert!(tail.is_empty());
                    }
                    assert_eq!(socket.read(&mut [0; 1]).expect("server EOF"), 0);
                },
            )
            .expect("server execution");
    }
}
