use std::{convert, net::TcpStream};

use common::sync::{Client, try_build, wire::Window};
use dope::{
    core::{
        driver::{life::recovery, route::KeyTag},
        io::socket::option::Switch,
    },
    runtime::{
        executor::session::Completion,
        topology::{self, lending},
    },
};
use dope_test::reclaim;
use sark_h2::conn::config::{Config, Settings};

pub mod common;

use std::{cell::Cell, pin::Pin};

use dope::{
    core::{
        driver::{
            route::{FrameworkTag, Tag, kind},
            settings,
        },
        io::socket,
    },
    manifold::network::listener::config,
    net::tcp,
};
use dope_test::harness::server::Pair;
use o3::{
    base::cell::region,
    buffer::{
        resident::{self, Shared},
        storage,
    },
};
use sark_h2::{ClientRole, conn::machine, server, server::sync};

use crate::common::{
    drained::Drained,
    sync::{LIMITS, Router, State, StaticRouter},
};

#[test]
fn static_uses_zero_flights() {
    use std::{io, num::NonZero, time::Duration};

    use dope::{core::driver::route::table::conn, manifold::app::env::quota};
    use sark_h2::server::runtime;

    let harness = Pair::bind().expect("H2 router harness");
    let plan = runtime::Plan::<KeyTag<0>>::new(
        config::Prepared {
            bind: harness.addr(),
            connections: conn::Capacity::fixed::<1>(),
            per_ip: Some(quota::PerIp::from(NonZero::<u32>::MIN)),
            direct_flights: 0.into(),
            backlog: socket::Backlog::fixed::<1>(),
            stream: tcp::StreamConfig {
                no_delay: Some(Switch::On),
                ..Default::default()
            },
            transport: tcp::Config::for_tcp(Some(socket::Reuse::Port), None),
            egress: Default::default(),
        },
        settings::Receive::fixed::<2, { 16 << 10 }>(),
        server::Config {
            max_request_body_bytes: 1,
            max_request_body_chunks: 1,
            max_connection_body_bytes: 1,
            max_connection_body_chunks: 1,
            max_outbound_bytes: 16 << 10,
        },
    )
    .expect("static router plan");
    let mut client = machine::Connection::<ClientRole>::try_new().expect("client fixture");
    let request = Client::new(&mut client)
        .try_get(b"/static")
        .expect("sync fixture")
        .as_slice()
        .to_vec();
    harness
        .check(
            move |context, listener| {
                let prepared = server::Server::new(StaticRouter::new(), plan)
                    .prepare_on(listener)
                    .expect("H2 layout");
                let mut reports = reclaim::Reports::<1>::new(
                    prepared.layout(),
                    [<FrameworkTag<kind::Any> as Tag>::ROUTE],
                );
                let mut finish = Completion::new();
                let result = prepared.enter_with(
                    context,
                    |worker| {
                        worker
                            .with(lending::Run(&mut finish))
                            .map_err(topology::Failure::Install)?
                            .map_err(|error| error.map_install(|never| match never {}))
                    },
                    recovery::Policy::new(
                        |report| reports.push(report),
                        |failure: recovery::Recovery<'_, '_>| -> convert::Infallible {
                            panic!("native H2 recovery: {:?}", failure.cause())
                        },
                    ),
                    |result| {
                        assert!(matches!(result, Ok(Ok(_))), "H2 server shutdown");
                        Ok::<(), convert::Infallible>(())
                    },
                );
                assert!(
                    result.is_ok(),
                    "static router server must shut down cleanly"
                );
                assert!(
                    reports.events().expect("H2 retirement").is_empty(),
                    "unexpected reclaimed server operations: {reports:?}"
                );
                Ok::<(), convert::Infallible>(())
            },
            |address| {
                let mut stream = TcpStream::connect(address).expect("H2 router peer");
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("peer read timeout");
                io::Write::write_all(&mut stream, &request).expect("H2 request");
                let mut received = Vec::new();
                let mut chunk = [0_u8; 4096];
                while !received
                    .windows(b"static-only".len())
                    .any(|window| window == b"static-only")
                {
                    let read = io::Read::read(&mut stream, &mut chunk).expect("H2 response");
                    assert_ne!(read, 0, "server closed before the static response");
                    received.extend_from_slice(&chunk[..read]);
                }
                assert!(
                    received
                        .windows(b"static-only".len())
                        .any(|window| window == b"static-only")
                );
            },
            |result| assert!(result.is_ok()),
        )
        .expect("zero-flight static router E2E");
}

#[test]
fn head_tail_is_bounded() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let state = State {
            request_body: Cell::new(0),
            request_head: Cell::new(0),
            invalid_path: Cell::new(false),
            header_error: Cell::new(None),
            dispatches: Cell::new(0),
            large_body: storage::Shared::new(),
        };
        let router = Router::new(&state);
        let mut client = machine::Connection::<ClientRole>::try_new().expect("client fixture");
        let wire = Client::new(&mut client)
            .try_get(b"/items/42")
            .expect("sync fixture");
        let endpoint = try_build(router, LIMITS).expect("sync fixture");
        let budget = domain
            .get_mut()
            .try_budget(LIMITS.max_connection_body_bytes, token.proof())
            .expect("resident budget");
        let mut server = sync::Conn::try_new(&endpoint).expect("sync connection");
        server
            .ingress()
            .try_bind(budget)
            .expect("connection retention");

        server
            .ingress()
            .ingest_owned(Shared::from(wire))
            .expect("the item request fits configured ingress credit");
        server.drain_events();

        assert_ne!(state.request_head.get(), 0);
        let drained = Drained::try_for(&mut server).expect("H2 response capture");
        assert!(drained.retained.is_empty());
        assert_eq!(drained.try_all().expect("H2 response observation"), b"42");
    });
}

#[test]
fn named_path_round_trips() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let state = State {
            request_body: Cell::new(0),
            request_head: Cell::new(0),
            invalid_path: Cell::new(false),
            header_error: Cell::new(None),
            dispatches: Cell::new(0),
            large_body: storage::Shared::new(),
        };
        let router = Router::new(&state);
        let mut client = machine::Connection::<ClientRole>::try_new().expect("client fixture");
        let wire = Client::new(&mut client)
            .try_get(b"/users/beta/teams/alpha")
            .expect("sync fixture");
        let endpoint = try_build(router, LIMITS).expect("sync fixture");
        let budget = domain
            .get_mut()
            .try_budget(LIMITS.max_connection_body_bytes, token.proof())
            .expect("resident budget");
        let mut server = sync::Conn::try_new(&endpoint).expect("sync connection");
        server
            .ingress()
            .try_bind(budget)
            .expect("connection retention");

        server
            .ingress()
            .ingest_owned(Shared::from(wire))
            .expect("the named path request fits configured ingress credit");
        server.drain_events();

        assert_ne!(state.request_head.get(), 0);
        let drained = Drained::try_for(&mut server).expect("H2 response capture");
        assert!(drained.retained.is_empty());
        assert_eq!(drained.try_all().expect("H2 response observation"), b"beta");
        assert!(
            !state.invalid_path.get(),
            "named captures must bind by name"
        );
    });
}

#[test]
fn backpressure_bounds_tail() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let state = State {
            request_body: Cell::new(0),
            request_head: Cell::new(0),
            invalid_path: Cell::new(false),
            header_error: Cell::new(None),
            dispatches: Cell::new(0),
            large_body: storage::Shared::new(),
        };
        let router = Router::new(&state);
        let settings = Settings {
            initial_window_size: 4,
            ..Settings::default()
        };
        let mut client = machine::Connection::<ClientRole>::with_config(Config {
            local_settings: settings,
            recv_window_target: 65_535,
            ..Default::default()
        })
        .expect("small receive window");
        let wire = Client::new(&mut client)
            .try_post(b"/echo", b"backpressure-body")
            .expect("sync fixture");
        let endpoint = try_build(router, LIMITS).expect("sync fixture");
        let budget = domain
            .get_mut()
            .try_budget(LIMITS.max_connection_body_bytes, token.proof())
            .expect("resident budget");
        let mut server = sync::Conn::try_new(&endpoint).expect("sync connection");
        server
            .ingress()
            .try_bind(budget)
            .expect("connection retention");

        server
            .ingress()
            .ingest_owned(Shared::from(wire))
            .expect("the backpressured request fits configured ingress credit");
        server.drain_events();
        assert_ne!(state.request_body.get(), 0);
        let first = Drained::try_for(&mut server).expect("H2 response capture");
        assert!(first.retained.is_empty());
        assert_eq!(first.try_all().expect("H2 response observation"), b"back");

        server
            .ingress()
            .ingest(&Window::new(1, 64).encode())
            .expect("the stream window update is a complete H2 frame");
        server.drain_events();
        let second = Drained::try_for(&mut server).expect("H2 response capture");
        assert!(second.retained.is_empty());
        assert_eq!(
            second.try_all().expect("H2 response observation"),
            b"pressure-body"
        );
    });
}
