pub mod common;

use std::{
    cell::Cell,
    convert,
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use common::server::recover;
use common::{
    client::runtime::sockets,
    server,
    support::{self, Entry},
};
use dope::core::driver::{
    life::recovery::Policy,
    route::{FrameworkTag, Tag, kind},
    settings::claim::Needs,
};
use dope_test::{harness::server::Pair, reclaim};
use dope_tls::tls::endpoint;
use sark_fiber::Config;
use support::{Mode, Routes};

fn check_plain(bind: SocketAddr) -> Result<(), sockets::Error> {
    let mut client = sockets::Plain::connect(bind)?;
    client.request("/hello", bind)?;
    let response = client.response()?;
    let reply = sockets::Response::try_parse(&response)?;
    assert_eq!((reply.status, reply.body), (200, b"hello".as_slice()));
    client.request("/hello", bind)?;
    let response = client.response()?;
    let reply = sockets::Response::try_parse(&response)?;
    assert_eq!((reply.status, reply.body), (200, b"hello".as_slice()));
    Ok(())
}

fn check_tls(bind: SocketAddr) -> Result<(), sockets::Error> {
    let mut client = sockets::Tls::connect(
        bind,
        support::tls().client().expect("TLS client config"),
        Ipv4Addr::LOCALHOST.into(),
    )?;
    let body = b"discard only decrypted bytes";
    let head = format!(
        "POST /hello HTTP/1.1\r\nHost: {bind}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    client.write(head.as_bytes())?;
    let response = client.response()?;
    let reply = sockets::Response::try_parse(&response)?;
    assert_eq!((reply.status, reply.body), (200, b"hello".as_slice()));
    let mut tail = body.to_vec();
    tail.extend_from_slice(format!("GET /hello HTTP/1.1\r\nHost: {bind}\r\n\r\n").as_bytes());
    client.write(&tail)?;
    let response = client.response()?;
    let reply = sockets::Response::try_parse(&response)?;
    assert_eq!((reply.status, reply.body), (200, b"hello".as_slice()));
    let chunks = 128;
    let head = format!(
        "POST /hello HTTP/1.1\r\nHost: {bind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        sockets::UPLOAD_CHUNK.len() * chunks
    );
    client.write(head.as_bytes())?;
    let response = client.response()?;
    let reply = sockets::Response::try_parse(&response)?;
    assert_eq!((reply.status, reply.body), (200, b"hello".as_slice()));
    for _ in 0..chunks {
        client.write(&sockets::UPLOAD_CHUNK)?;
    }
    assert!(matches!(
        client.response(),
        Err(sockets::Error::Closed { buffered: 0 })
    ));
    drop(client);
    Ok(())
}

fn keepalive(mode: Mode, entry: Entry) {
    let harness = (Pair::bind()).expect("bind ephemeral loopback harness");
    let bind = harness.addr();
    match mode {
        Mode::Plain => {
            let server = server::Plan::new(bind, Duration::from_secs(10)).build();
            let result = match entry {
                Entry::Configured => harness.run_unbound(
                    move |context| {
                        let config = Config::new(server::MAX_CONNECTIONS);
                        let mut reports = reclaim::Reports::new(
                            Needs::from_tree(&server)
                                .and_then(|needs| needs.resolve::<sark_fiber::Throughput>())
                                .expect("runtime resource claims"),
                            [<FrameworkTag<kind::Any> as Tag>::ROUTE],
                        );
                        let errors = Cell::new(Vec::new());
                        let result = Routes::serve(
                            server,
                            context,
                            server::State(&errors),
                            config,
                            Policy::new(|event| reports.push(event), recover),
                            |outcome| {
                                outcome
                                    .as_ref()
                                    .map(|_| ())
                                    .map_err(|error| error.cause())
                                    .expect("server execution");
                            },
                        );
                        assert!(result.is_ok(), "configured HTTP server shutdown");
                        drop(result);
                        let errors = errors.into_inner();
                        assert!(errors.is_empty(), "TLS failures: {errors:?}");
                        assert!(
                            reports.events().expect("reclaimed resources").is_empty(),
                            "unexpected reports server operations: {reports:?}"
                        );
                        Ok::<(), convert::Infallible>(())
                    },
                    check_plain,
                ),
                Entry::Bound => harness.run(
                    move |context, listener| {
                        let config = Config::new(server::MAX_CONNECTIONS);
                        let mut reports = reclaim::Reports::new(
                            Needs::from_tree(&server)
                                .and_then(|needs| needs.resolve::<sark_fiber::Throughput>())
                                .expect("runtime resource claims"),
                            [<FrameworkTag<kind::Any> as Tag>::ROUTE],
                        );
                        let errors = Cell::new(Vec::new());
                        let result = Routes::serve(
                            server.with_bound(listener),
                            context,
                            server::State(&errors),
                            config,
                            Policy::new(|event| reports.push(event), recover),
                            |outcome| {
                                outcome
                                    .as_ref()
                                    .map(|_| ())
                                    .map_err(|error| error.cause())
                                    .expect("server execution");
                            },
                        );
                        assert!(result.is_ok(), "bound HTTP server shutdown");
                        drop(result);
                        let errors = errors.into_inner();
                        assert!(errors.is_empty(), "TLS failures: {errors:?}");
                        assert!(
                            reports.events().expect("reclaimed resources").is_empty(),
                            "unexpected reports server operations: {reports:?}"
                        );
                        Ok::<(), convert::Infallible>(())
                    },
                    check_plain,
                ),
            };
            assert!(
                matches!(
                    result.map(|result| result.expect("plain keepalive")),
                    Ok(())
                ),
                "plain keepalive"
            );
        }
        Mode::Tls => {
            let server = server::Plan::new(bind, Duration::from_secs(10)).build();
            let result = match entry {
                Entry::Configured => harness.run_unbound(
                    move |context| {
                        let wire = endpoint::Config::from(
                            support::tls().server().expect("TLS server config"),
                        );
                        let config = Config::new(server::MAX_CONNECTIONS);
                        let mut reports = reclaim::Reports::new(
                            Needs::from_tree(&server)
                                .and_then(|needs| needs.resolve::<sark_fiber::Throughput>())
                                .expect("runtime resource claims"),
                            [<FrameworkTag<kind::Any> as Tag>::ROUTE],
                        );
                        let errors = Cell::new(Vec::new());
                        let result = Routes::serve_with(
                            server,
                            wire,
                            context,
                            server::State(&errors),
                            config,
                            Policy::new(|event| reports.push(event), recover),
                            |outcome| {
                                outcome
                                    .as_ref()
                                    .map(|_| ())
                                    .map_err(|error| error.cause())
                                    .expect("server execution");
                            },
                        );
                        assert!(result.is_ok(), "configured TLS server shutdown");
                        drop(result);
                        let errors = errors.into_inner();
                        assert!(errors.is_empty(), "TLS failures: {errors:?}");
                        assert!(
                            reports.events().expect("reclaimed resources").is_empty(),
                            "unexpected reports server operations: {reports:?}"
                        );
                        Ok::<(), convert::Infallible>(())
                    },
                    check_tls,
                ),
                Entry::Bound => harness.run(
                    move |context, listener| {
                        let wire = endpoint::Config::from(
                            support::tls().server().expect("TLS server config"),
                        );
                        let config = Config::new(server::MAX_CONNECTIONS);
                        let mut reports = reclaim::Reports::new(
                            Needs::from_tree(&server)
                                .and_then(|needs| needs.resolve::<sark_fiber::Throughput>())
                                .expect("runtime resource claims"),
                            [<FrameworkTag<kind::Any> as Tag>::ROUTE],
                        );
                        let errors = Cell::new(Vec::new());
                        let result = Routes::serve_with(
                            server.with_bound(listener),
                            wire,
                            context,
                            server::State(&errors),
                            config,
                            Policy::new(|event| reports.push(event), recover),
                            |outcome| {
                                outcome
                                    .as_ref()
                                    .map(|_| ())
                                    .map_err(|error| error.cause())
                                    .expect("server execution");
                            },
                        );
                        assert!(result.is_ok(), "bound TLS server shutdown");
                        drop(result);
                        let errors = errors.into_inner();
                        assert!(errors.is_empty(), "TLS failures: {errors:?}");
                        assert!(
                            reports.events().expect("reclaimed resources").is_empty(),
                            "unexpected reports server operations: {reports:?}"
                        );
                        Ok::<(), convert::Infallible>(())
                    },
                    check_tls,
                ),
            };
            assert!(
                matches!(result.map(|result| result.expect("TLS keepalive")), Ok(())),
                "TLS keepalive"
            );
        }
    }
}

#[test]
fn https_streams_large_body() {
    let harness = (Pair::bind()).expect("bind ephemeral loopback harness");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();
    let result = harness.run(
        move |context, listener| {
            let wire = endpoint::Config::from(support::tls().server().expect("TLS server config"));
            let mut reports = reclaim::Reports::new(
                Needs::from_tree(&server)
                    .and_then(|needs| needs.resolve::<sark_fiber::Throughput>())
                    .expect("runtime resource claims"),
                [<FrameworkTag<kind::Any> as Tag>::ROUTE],
            );
            let errors = Cell::new(Vec::new());
            let result = Routes::serve_with(
                server.with_bound(listener),
                wire,
                context,
                server::State(&errors),
                Config::new(server::MAX_CONNECTIONS),
                Policy::new(|event| reports.push(event), recover),
                |outcome| {
                    outcome
                        .as_ref()
                        .map(|_| ())
                        .map_err(|error| error.cause())
                        .expect("server execution");
                },
            );
            assert!(result.is_ok(), "TLS streaming server shutdown");
            drop(result);
            let errors = errors.into_inner();
            assert!(errors.is_empty(), "TLS failures: {errors:?}");
            assert!(
                reports.events().expect("reclaimed resources").is_empty(),
                "unexpected reports server operations: {reports:?}"
            );
            Ok::<(), convert::Infallible>(())
        },
        |bind| {
            let mut client = sockets::Tls::connect(
                bind,
                support::tls().client().expect("TLS client config"),
                Ipv4Addr::LOCALHOST.into(),
            )?;
            client.request("/big", bind)?;
            let response = client.response()?;
            let reply = sockets::Response::try_parse(&response)?;
            assert_eq!(reply.status, 200);
            assert_eq!(reply.body.len(), support::BIG_LEN);
            assert!(reply.body.iter().all(|&byte| byte == b'x'));
            drop(client);
            Ok::<_, sockets::Error>(())
        },
    );
    assert!(
        matches!(
            result.map(|result| result.expect("TLS large response")),
            Ok(())
        ),
        "TLS large response"
    );
}

#[test]
fn modes_reuse_connections() {
    for mode in [Mode::Plain, Mode::Tls] {
        for entry in [Entry::Configured, Entry::Bound] {
            keepalive(mode, entry);
        }
    }
}
