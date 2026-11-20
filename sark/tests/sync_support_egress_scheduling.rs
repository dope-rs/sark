pub mod common;

use dope_test::alloc::Tracker;
use o3::buffer::{resident::Shared, storage};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    conn::{self, config::Settings},
    frame::numbers::{ErrorCode, Limit},
    server::sync::Endpoint,
};
use sark_http::field::{self, Value};

use crate::common::{
    drained::Drained,
    sync::{
        Client, LIMITS, Router, State, backpressure_plan, connection, try_build, try_peer,
        with_domain,
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn streams_round_robin() {
    with_domain(|domain, proof| {
        let body = storage::Owned::try_filled(150_000, b'x')
            .expect("the multiplexed response body fits test storage")
            .freeze();
        let base = body.as_slice().as_ptr().addr();
        let body_len = body.len();
        let state = State::new(body);
        let router = Router::new(&state);
        let peer = Settings {
            max_frame_size: Limit::MAX,
            initial_window_size: 1_000_000,
            ..Settings::default()
        };
        let mut client = try_peer(peer, 1_000_000).expect("large multiplexed receive windows");
        let request = Client::get_head(field::literal!(Value::try_bytes(b"/large")));
        let first = client
            .streams()
            .headers()
            .request(request, Boundary::End)
            .expect("the first large request fits the client stream table");
        let second = client
            .streams()
            .headers()
            .request(request, Boundary::End)
            .expect("the second large request fits the client stream table");
        let wire = Client::new(&mut client).try_take().expect("sync fixture");

        let plan = backpressure_plan(1);
        let endpoint = Endpoint::try_with(router, plan, 8)
            .expect("eight streams fit the multiplexed endpoint layout");
        let mut server = connection(&endpoint, domain, proof, wire.len());

        server
            .ingress()
            .ingest_owned(Shared::from(wire))
            .expect("both large requests fit configured ingress credit");
        server.drain_events();
        let mut sent = [0_usize; 2];
        let mut retained = [0_usize; 2];
        let mut overlap = None;
        let account = |drained: Drained,
                       sent: &mut [usize; 2],
                       retained: &mut [usize; 2],
                       overlap: &mut Option<usize>| {
            drained
                .try_visit(|part| {
                    let slot = if part.stream == first {
                        0
                    } else {
                        assert!(
                            (part.stream) == (second),
                            "round-robin drain emitted unadmitted stream"
                        );
                        overlap.get_or_insert(sent[0]);
                        1
                    };
                    if let Some(owner) = part.owner {
                        assert_eq!(owner.pointer, base + sent[slot]);
                        retained[slot] += owner.len;
                    }
                    sent[slot] += part.payload.len();
                    assert!(sent[slot] <= body_len);
                    Ok(())
                })
                .expect("H2 response observation");
        };
        account(
            Drained::try_for(&mut server).expect("H2 response capture"),
            &mut sent,
            &mut retained,
            &mut overlap,
        );

        for _ in 0..64 {
            let ((), allocs) = Tracker::usage(|| {
                server.pump();
            });
            assert_eq!(allocs, (0, 0));
            account(
                Drained::try_for(&mut server).expect("H2 response capture"),
                &mut sent,
                &mut retained,
                &mut overlap,
            );
            if sent == [body_len, body_len] {
                break;
            }
        }

        assert_eq!(sent, [body_len, body_len]);
        assert!(
            retained
                .into_iter()
                .all(|bytes| { body_len - bytes <= conn::MAX_MATERIALIZED_RESPONSE_TAIL })
        );
        assert_eq!(state.dispatches.get(), 2);
        assert!(
            overlap.is_some_and(|at| at < body_len),
            "the second stream must receive DATA before the first completes"
        );
    });
}

#[test]
fn reset_wakes_next() {
    with_domain(|domain, proof| {
        let body = storage::Owned::try_filled(150_000, b'x')
            .expect("the reset response body fits test storage")
            .freeze();
        let base = body.as_slice().as_ptr().addr();
        let end = base + body.len();
        let state = State::new(body);
        let router = Router::new(&state);
        let mut client =
            try_peer(Settings::DEFAULT, 65_535).expect("standard connection receive window");
        let key = client
            .streams()
            .headers()
            .request_key(
                Client::get_head(field::literal!(Value::try_bytes(b"/large"))),
                Boundary::End,
            )
            .expect("the first reset candidate fits the client stream table");
        let head = Client::new(&mut client).try_take().expect("sync fixture");
        let endpoint = try_build(router, LIMITS).expect("sync fixture");
        let mut server = connection(&endpoint, domain, proof, LIMITS.max_connection_body_bytes);

        server
            .ingress()
            .ingest_owned(Shared::from(head))
            .expect("the first reset candidate fits ingress storage");
        server.drain_events();
        assert_eq!(state.dispatches.get(), 1);
        let reserved = Settings::DEFAULT.max_frame_size.into_usize();
        assert_eq!(
            server.status().send_window().available(),
            65_535 - reserved,
            "one scheduling turn may reserve exactly one peer-sized DATA frame"
        );

        let second = client
            .streams()
            .headers()
            .request(
                Client::get_head(field::literal!(Value::try_bytes(b"/items/survivor"))),
                Boundary::End,
            )
            .expect("the survivor request fits the client stream table");
        let next = Client::new(&mut client).try_take().expect("sync fixture");
        server
            .ingress()
            .ingest_owned(Shared::from(next))
            .expect("the survivor request fits ingress storage");
        server.drain_events();
        let next_ptr = state.request_head.get();
        assert_ne!(next_ptr, 0);
        let before = server.status().send_window().available();
        assert!(before < 65_535 - b"survivor".len());

        client.streams().cancel(key, ErrorCode::Cancel);
        let reset = Client::new(&mut client).try_take().expect("sync fixture");
        server
            .ingress()
            .ingest_owned(Shared::from(reset))
            .expect("the reset frame fits ingress storage");
        server.drain_events();

        assert_eq!(
            server.status().send_window().available(),
            65_535 - b"survivor".len(),
            "RST must cancel the first reservation and synchronously wake the connection-flow lane"
        );
        assert!(
            !server.status().has_stream(second),
            "the surviving response must have reached its terminal DATA frame"
        );

        let drained = Drained::try_for(&mut server).expect("H2 response capture");
        assert!(
            drained
                .retained
                .iter()
                .all(|chunk| !(base..end).contains(&chunk.pointer)),
            "canceled retained DATA must never cross the transport handoff"
        );
        assert_eq!(
            drained.try_data(second).expect("H2 response observation"),
            b"survivor"
        );
        assert_eq!(
            drained.retained_len(),
            0,
            "the bounded survivor is materialized"
        );
        assert_ne!(next_ptr, base);
    });
}
