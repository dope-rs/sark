pub mod common;

use dope_test::alloc::Tracker;
use o3::buffer::{resident::Shared, storage};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    conn::DataOutcome,
    frame::numbers::ErrorCode,
    server::{self, sync::Endpoint},
};
use sark_http::field::{self, Value};

use crate::common::{
    drained::Drained,
    sync::{
        Client, DeniedState, GuardRouter, LIMITS, OUTBOUND_LIMIT, Router, State, StaticRouter,
        backpressure_plan, client, connection, try_build, with_domain,
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn route_bounds_tail() {
    with_domain(|domain, proof| {
        let state = State::new(storage::Shared::new());
        let router = Router::new(&state);
        let mut client = client();
        let wire = Client::new(&mut client)
            .try_post(b"/echo", b"owner-preserved")
            .expect("sync fixture");
        let endpoint = try_build(router, LIMITS).expect("sync fixture");
        let mut server = connection(&endpoint, domain, proof, LIMITS.max_connection_body_bytes);

        server
            .ingress()
            .ingest_owned(Shared::from(wire))
            .expect("the retained request fits configured ingress credit");
        server.drain_events();

        assert_ne!(state.request_body.get(), 0);
        let drained = Drained::try_for(&mut server).expect("H2 response capture");
        assert!(drained.retained.is_empty());
        assert_eq!(
            drained.try_all().expect("H2 response observation"),
            b"owner-preserved"
        );
    });
}

#[test]
fn static_has_no_owner() {
    with_domain(|domain, proof| {
        let router = StaticRouter::new();
        let mut client = client();
        let wire = Client::new(&mut client)
            .try_get(b"/static")
            .expect("sync fixture");
        let endpoint = try_build(router, LIMITS).expect("sync fixture");
        let mut server = connection(&endpoint, domain, proof, LIMITS.max_connection_body_bytes);
        server
            .ingress()
            .ingest_owned(Shared::from(wire))
            .expect("the static request fits configured ingress credit");
        server.drain_events();

        let drained = Drained::try_for(&mut server).expect("H2 response capture");
        assert!(drained.retained.is_empty());
        assert_eq!(
            drained.try_all().expect("H2 response observation"),
            b"static-only"
        );
    });
}

#[test]
fn oversize_skips_guard() {
    with_domain(|domain, proof| {
        let state = DeniedState::default();
        let router = GuardRouter::new(&state);
        let mut client = client();
        let wire = Client::new(&mut client)
            .try_post(b"/guarded", b"larger-than-limit")
            .expect("sync fixture");
        let limits = server::Config {
            max_request_body_bytes: 1,
            max_connection_body_bytes: 1,
            ..LIMITS
        };
        let endpoint = try_build(router, limits).expect("sync fixture");
        let mut server = connection(&endpoint, domain, proof, LIMITS.max_connection_body_bytes);
        server
            .ingress()
            .ingest_owned(Shared::from(wire))
            .expect("the oversized declaration frame fits ingress storage");
        server.drain_events();

        let drained = Drained::try_for(&mut server).expect("H2 response capture");
        assert!(drained.retained.is_empty());
        assert!(
            drained
                .try_all()
                .expect("H2 response observation")
                .is_empty()
        );
        assert!(
            (drained.try_reset().expect("H2 response observation"))
                == (Some(ErrorCode::EnhanceYourCalm))
        );
        assert_eq!(state.guards.get(), 0);
        assert_eq!(state.dispatches.get(), 0);
    });
}

#[test]
fn finished_body_rejected() {
    with_domain(|domain, proof| {
        let state = DeniedState::default();
        let router = GuardRouter::new(&state);
        let mut client = client();
        client
            .streams()
            .headers()
            .request(
                Client::post_head(
                    field::literal!(Value::try_bytes(b"/guarded")),
                    field::literal!(Value::try_bytes(b"1")),
                ),
                Boundary::End,
            )
            .expect("the finished declaration fits the client stream table");
        let wire = Client::new(&mut client).try_take().expect("sync fixture");
        let endpoint = try_build(router, LIMITS).expect("sync fixture");
        let mut server = connection(&endpoint, domain, proof, LIMITS.max_connection_body_bytes);
        server
            .ingress()
            .ingest_owned(Shared::from(wire))
            .expect("the finished declaration frame fits ingress storage");
        server.drain_events();

        let drained = Drained::try_for(&mut server).expect("H2 response capture");
        assert!(drained.retained.is_empty());
        assert!(
            drained
                .try_all()
                .expect("H2 response observation")
                .is_empty()
        );
        assert!(
            (drained.try_reset().expect("H2 response observation"))
                == (Some(ErrorCode::ProtocolError))
        );
        assert_eq!(state.guards.get(), 0);
        assert_eq!(state.dispatches.get(), 0);
    });
}

#[test]
fn guard_precedes_body() {
    with_domain(|domain, proof| {
        let state = DeniedState::default();
        let router = GuardRouter::new(&state);
        let mut client = client();
        let stream = client
            .streams()
            .headers()
            .request(
                Client::post_head(
                    field::literal!(Value::try_bytes(b"/guarded")),
                    field::literal!(Value::try_bytes(b"4")),
                ),
                Boundary::More,
            )
            .expect("the guarded request head fits the client stream table");
        let head = Client::new(&mut client).try_take().expect("sync fixture");
        let endpoint = try_build(router, LIMITS).expect("sync fixture");
        let mut server = connection(&endpoint, domain, proof, LIMITS.max_connection_body_bytes);

        server
            .ingress()
            .ingest_owned(Shared::from(head))
            .expect("the guarded request head fits ingress storage");
        server.drain_events();

        assert_eq!(state.guards.get(), 1);
        assert_eq!(state.dispatches.get(), 0);
        let rejected = Drained::try_for(&mut server).expect("H2 response capture");
        assert_eq!(
            rejected.try_all().expect("H2 response observation"),
            b"denied"
        );
        assert!((rejected.try_reset().expect("H2 response observation")).is_none());

        assert!(matches!(
            client
                .streams()
                .try_send(stream, b"late", Boundary::End)
                .expect("the client window admits the late body"),
            DataOutcome::Sent(sent) if sent.get() == 4
        ));
        let body = Client::new(&mut client).try_take().expect("sync fixture");
        server
            .ingress()
            .ingest_owned(Shared::from(body))
            .expect("the late body fits ingress storage");
        server.drain_events();

        assert_eq!(state.guards.get(), 1);
        assert_eq!(state.dispatches.get(), 0);
        assert!(
            Drained::try_for(&mut server)
                .expect("H2 response capture")
                .try_all()
                .expect("H2 response observation")
                .is_empty()
        );
        assert!(!state.invalid_guard.get(), "guard method and path");
    });
}

#[test]
fn deferred_guard_is_once() {
    with_domain(|domain, proof| {
        let state = DeniedState::default();
        let router = GuardRouter::new(&state);
        let mut client = client();
        let value = vec![b'~'; 16_000];
        let occupied = Client::new(&mut client)
            .try_post(b"/occupy", &value)
            .expect("sync fixture");
        let guarded = client
            .streams()
            .headers()
            .request(
                Client::post_head(
                    field::literal!(Value::try_bytes(b"/guarded")),
                    field::literal!(Value::try_bytes(b"4")),
                ),
                Boundary::More,
            )
            .expect("the deferred guarded head fits the client stream table");
        assert!(matches!(
            client
                .streams()
                .try_send(guarded, b"la", Boundary::More)
                .expect("the client window admits the pending body prefix"),
            DataOutcome::Sent(sent) if sent.get() == 2
        ));
        let pending = Client::new(&mut client).try_take().expect("sync fixture");
        let body_bytes = 16 << 10;
        let plan = backpressure_plan(body_bytes);
        let endpoint = Endpoint::try_with(router, plan, 8)
            .expect("eight streams fit the test endpoint layout");
        let mut server = connection(&endpoint, domain, proof, body_bytes);

        server
            .ingress()
            .ingest_owned(Shared::from(occupied))
            .expect("the occupying body fits the full connection credit");
        server.drain_events();
        server
            .ingress()
            .ingest_owned(Shared::from(pending))
            .expect("the pending guarded body fits ingress storage");
        server.drain_events();
        assert_eq!(state.dispatches.get(), 0);
        assert_eq!(state.guards.get(), 0);

        Drained::try_for(&mut server).expect("H2 response capture");
        server.pump();
        assert_eq!(state.dispatches.get(), 1);
        assert_eq!(state.guards.get(), 0);
        assert!(server.output().len() > OUTBOUND_LIMIT);
        Drained::try_for(&mut server).expect("H2 response capture");

        let (report, allocs) = Tracker::usage(|| server.pump());
        assert!(report.turns != 0);
        assert_eq!(allocs, (0, 0));
        assert_eq!(state.dispatches.get(), 1);
        assert_eq!(state.guards.get(), 1);
        let rejected = Drained::try_for(&mut server).expect("H2 response capture");
        assert_eq!(
            rejected.try_all().expect("H2 response observation"),
            b"denied"
        );
        assert!((rejected.try_reset().expect("H2 response observation")).is_none());

        assert!(matches!(
            client
                .streams()
                .try_send(guarded, b"te", Boundary::End)
                .expect("the client window admits the pending body suffix"),
            DataOutcome::Sent(sent) if sent.get() == 2
        ));
        let finished = Client::new(&mut client).try_take().expect("sync fixture");
        server
            .ingress()
            .ingest_owned(Shared::from(finished))
            .expect("the finished guarded body fits ingress storage");
        server.drain_events();
        assert_eq!(state.guards.get(), 1);
        assert_eq!(state.dispatches.get(), 1);
        assert!(!state.invalid_guard.get(), "guard method and path");
        assert_eq!(state.header_error.get(), None);
    });
}
