pub mod common;

use dope_test::alloc::Tracker;
use o3::buffer::{resident::Shared, storage};
use sark_h2::{
    conn::{self, config::Settings},
    frame::numbers::Limit,
    server::sync::{self, Endpoint},
    stream::Id,
};

use crate::common::{
    drained::Drained,
    sync::{
        Client, LIMITS, OUTBOUND_LIMIT, Router, State, backpressure_plan, client, try_build,
        try_peer, wire::Window, with_domain,
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn warm_dispatch_is_bounded() {
    for (payload, copied) in [
        (b"steady-state".as_slice(), true),
        ([b'x'; 64].as_slice(), false),
    ] {
        with_domain(|domain, proof| {
            let budget = domain
                .try_budget(payload.len(), proof)
                .expect("response body retention budget");
            let state = State::new(storage::Shared::new());
            let router = Router::new(&state);
            let mut client = client();
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
    with_domain(|domain, proof| {
        let state = State::new(storage::Shared::new());
        let router = Router::new(&state);
        let mut client = client();
        let value = vec![b'~'; 16_000];
        let wire = Client::new(&mut client)
            .try_post(b"/headers", &value)
            .expect("sync fixture");
        let plan = backpressure_plan(16 << 10);
        let resident = plan.resident_len();
        let endpoint = Endpoint::try_with(router, plan, 256)
            .expect("the header test stream count fits the endpoint layout");
        let mut server = sync::Conn::try_new(&endpoint).expect("sync connection");
        let budget = domain
            .try_budget(resident, proof)
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
        assert!(server.output().len() > OUTBOUND_LIMIT);
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
    with_domain(|domain, proof| {
        let body = storage::Owned::try_filled(100_000, b'x')
            .expect("the large response body fits test storage")
            .freeze();
        let base = body.as_slice().as_ptr().addr();
        let body_len = body.len();
        let state = State::new(body);
        let router = Router::new(&state);
        let peer = Settings {
            max_frame_size: Limit::MAX,
            ..Settings::default()
        };
        let mut client = try_peer(peer, 65_535).expect("large peer frame setting");
        let wire = Client::new(&mut client)
            .try_get(b"/large")
            .expect("sync fixture");
        let plan = backpressure_plan(1);
        let resident = plan.resident_len();
        let endpoint = Endpoint::try_with(router, plan, 8)
            .expect("eight streams fit the large-body endpoint layout");
        let mut server = sync::Conn::try_new(&endpoint).expect("sync connection");
        let budget = domain
            .try_budget(resident, proof)
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
