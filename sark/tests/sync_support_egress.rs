use common::sync::{Client, try_build, wire::Window};
use sark_h2::{
    conn,
    conn::{config, config::Settings},
    frame::numbers::Limit,
    stream::Id,
};

pub mod common;

use std::{cell::Cell, pin::Pin};

use dope::core::driver::settings;
use dope_test::alloc::Tracker;
use o3::{
    base::cell::region,
    buffer::{
        resident::{self, Shared},
        storage,
    },
};
use sark_h2::{
    ClientRole,
    conn::machine,
    server,
    server::{sync, sync::Endpoint},
};

use crate::common::{
    drained::Drained,
    sync::{LIMITS, Router, State},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn warm_dispatch_is_bounded() {
    for (payload, copied) in [
        (b"steady-state".as_slice(), true),
        ([b'x'; 64].as_slice(), false),
    ] {
        let mut domain = resident::Domain::new();
        region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
            let budget = domain
                .get_mut()
                .try_budget(payload.len(), token.proof())
                .expect("response body retention budget");
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
            let endpoint = try_build(router, LIMITS).expect("sync fixture");
            let mut server = sync::Conn::try_new(&endpoint).expect("sync connection");
            server.ingress().try_bind(budget).expect("retention domain");
            let warm = Client::new(&mut client)
                .try_post(b"/echo", b"warm")
                .expect("sync fixture");
            server
                .ingress()
                .ingest_owned(Shared::from(warm))
                .expect("the warm request fits configured ingress credit");
            server.drain_events();
            let warm = Drained::try_for(&mut server).expect("H2 response capture");
            assert!(warm.retained.is_empty());
            assert_eq!(warm.try_all().expect("H2 response observation"), b"warm");
            let wire = Client::new(&mut client)
                .try_post(b"/echo", payload)
                .expect("sync fixture");
            assert_eq!(wire.len() > 2 * payload.len(), copied);
            let offset = wire
                .as_slice()
                .windows(payload.len())
                .position(|bytes| bytes == payload)
                .expect("request body on wire");
            let pointer = wire.as_slice().as_ptr().wrapping_add(offset).addr();
            let ((), allocs) = Tracker::usage(|| {
                server
                    .ingress()
                    .ingest_owned(Shared::from(wire))
                    .expect("the steady request fits configured ingress credit");
                assert_eq!(budget.resident(), if copied { payload.len() } else { 0 });
                server.drain_events();
            });
            if copied {
                assert_eq!(allocs.0, 1);
                assert_ne!(state.request_body.get(), pointer);
            } else {
                assert_eq!(allocs, (0, 0));
                assert_eq!(state.request_body.get(), pointer);
            }
            assert_eq!(budget.resident(), 0);
            let steady = Drained::try_for(&mut server).expect("H2 response capture");
            assert!(steady.retained.is_empty());
            assert_eq!(steady.try_all().expect("H2 response observation"), payload);
        });
    }
}

#[test]
fn head_waits_without_alloc() {
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
        let value = vec![b'~'; 16_000];
        let wire = Client::new(&mut client)
            .try_post(b"/headers", &value)
            .expect("sync fixture");
        let limits = server::Config {
            max_request_body_bytes: 16 << 10,
            max_request_body_chunks: 4 << 10,
            max_connection_body_bytes: 16 << 10,
            max_connection_body_chunks: 16 << 10,
            max_outbound_bytes: 1,
        };
        let receive = settings::Receive::fixed::<2, { 16 << 10 }>();
        let plan = server::Plan::new(limits, receive).expect("test limits fit the receive plan");
        let resident = plan.resident_len();
        let endpoint = Endpoint::try_with(router, plan, 256)
            .expect("the header test stream count fits the endpoint layout");
        let mut server = sync::Conn::try_new(&endpoint).expect("sync connection");
        let budget = domain
            .get_mut()
            .try_budget(resident, token.proof())
            .expect("planned receive budget");
        server.ingress().try_bind(budget).expect("retention domain");

        server
            .ingress()
            .ingest_owned(Shared::from(wire))
            .expect("the large header request fits configured ingress credit");
        server.drain_events();
        assert_eq!(
            state.dispatches.get(),
            0,
            "initial SETTINGS applies backpressure"
        );

        Drained::try_for(&mut server).expect("H2 response capture");
        let ((), allocs) = Tracker::usage(|| {
            server.pump();
        });
        assert_eq!(allocs, (0, 0));
        assert_eq!(state.dispatches.get(), 1);
        assert!(server.output().len() > limits.max_outbound_bytes);
        assert!(
            !server
                .status()
                .has_stream(Id::new(1).expect("stream one is nonzero"))
        );
        Drained::try_for(&mut server).expect("H2 response capture");
        server.pump();
        assert_eq!(
            state.dispatches.get(),
            1,
            "the handler must never be replayed"
        );
        assert_eq!(state.header_error.get(), None);
    });
}

#[test]
fn large_body_keeps_owner() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let body = storage::Owned::try_filled(100_000, b'x')
            .expect("the large response body fits test storage")
            .freeze();
        let base = body.as_slice().as_ptr().addr();
        let body_len = body.len();
        let state = State {
            request_body: Cell::new(0),
            request_head: Cell::new(0),
            invalid_path: Cell::new(false),
            header_error: Cell::new(None),
            dispatches: Cell::new(0),
            large_body: body,
        };
        let router = Router::new(&state);
        let peer = Settings {
            max_frame_size: Limit::MAX,
            ..Settings::default()
        };
        let mut client = machine::Connection::<ClientRole>::with_config(config::Config {
            local_settings: peer,
            recv_window_target: 65_535,
            ..Default::default()
        })
        .expect("large peer frame setting");
        let wire = Client::new(&mut client)
            .try_get(b"/large")
            .expect("sync fixture");
        let limits = server::Config {
            max_request_body_bytes: 1,
            max_request_body_chunks: 4 << 10,
            max_connection_body_bytes: 1,
            max_connection_body_chunks: 16 << 10,
            max_outbound_bytes: 1,
        };
        let receive = settings::Receive::fixed::<2, { 16 << 10 }>();
        let plan = server::Plan::new(limits, receive).expect("test limits fit the receive plan");
        let resident = plan.resident_len();
        let endpoint = Endpoint::try_with(router, plan, 8)
            .expect("eight streams fit the large-body endpoint layout");
        let mut server = sync::Conn::try_new(&endpoint).expect("sync connection");
        let budget = domain
            .get_mut()
            .try_budget(resident, token.proof())
            .expect("planned receive budget");
        server.ingress().try_bind(budget).expect("retention domain");

        server
            .ingress()
            .ingest_owned(Shared::from(wire))
            .expect("the large response request fits configured ingress credit");
        server.drain_events();
        let stream = Id::new(1).expect("stream one is nonzero");
        let mut sent = 0;
        let mut held = Drained::try_for(&mut server)
            .expect("H2 response capture")
            .try_account(stream, base, &mut sent)
            .expect("H2 response observation");
        let ((), allocs) = Tracker::usage(|| {
            server.pump();
        });
        assert_eq!(allocs, (0, 0));
        assert_eq!(state.dispatches.get(), 1);
        held += Drained::try_for(&mut server)
            .expect("H2 response capture")
            .try_account(stream, base, &mut sent)
            .expect("H2 response observation");

        let mut released = false;
        while sent != body_len {
            let ((), allocs) = Tracker::usage(|| {
                server.pump();
            });
            assert_eq!(allocs, (0, 0));

            let before = sent;
            held += Drained::try_for(&mut server)
                .expect("H2 response capture")
                .try_account(stream, base, &mut sent)
                .expect("H2 response observation");
            if sent != before || sent == body_len {
                continue;
            }

            assert!(!released, "flow control may block only once in this test");
            assert_eq!(sent, 65_535);
            server
                .ingress()
                .ingest(&Window::new(0, 100_000).encode())
                .expect("the connection window update is a complete H2 frame");
            server
                .ingress()
                .ingest(&Window::new(1, 100_000).encode())
                .expect("the stream window update is a complete H2 frame");
            server.drain_events();
            released = true;
        }

        assert_eq!(sent, body_len);
        assert!(
            body_len - held <= conn::MAX_MATERIALIZED_RESPONSE_TAIL,
            "only the compile-time bounded response tail may be materialized"
        );
        assert!(released);
        assert_eq!(state.dispatches.get(), 1);
        assert!(
            !server
                .status()
                .has_stream(Id::new(1).expect("stream one is nonzero"))
        );
    });
}
