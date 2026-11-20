pub mod common;

use dope_test::alloc::Tracker;
use o3::buffer::{resident::Shared, storage};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    conn::DataOutcome,
    frame::numbers::ErrorCode,
    server::{self, sync},
};
use sark_http::field::{self, Value};

use crate::common::{
    drained::Drained,
    sync::{Client, LIMITS, Router, State, client, try_build, with_domain},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn fragment_retention_bound() {
    for (length, prefix, suffix, copied) in [
        (
            b"10".as_slice(),
            b"first".as_slice(),
            b"last!".as_slice(),
            true,
        ),
        (
            b"20".as_slice(),
            b"first-part".as_slice(),
            b"last-part!".as_slice(),
            false,
        ),
    ] {
        with_domain(|domain, proof| {
            let budget = domain
                .try_budget(prefix.len() + suffix.len(), proof)
                .expect("fragment retention budget");
            let state = State::new(storage::Shared::new());
            let router = Router::new(&state);
            let mut client = client();
            let stream = client
                .streams()
                .headers()
                .request(
                    Client::post_head(
                        field::literal!(Value::try_bytes(b"/echo")),
                        Value::try_bytes(length).expect("content length"),
                    ),
                    Boundary::More,
                )
                .expect("the fragmented request fits the client stream table");
            let head = Client::new(&mut client).try_take().expect("sync fixture");
            assert!(matches!(
                client.streams().try_send(stream, prefix, Boundary::More)
                    .expect("the client window admits the first fragment"),
                DataOutcome::Sent(sent) if sent.get() == prefix.len()
            ));
            let first = Client::new(&mut client).try_take().expect("sync fixture");
            assert!(matches!(
                client.streams().try_send(stream, suffix, Boundary::End)
                    .expect("the client window admits the final fragment"),
                DataOutcome::Sent(sent) if sent.get() == suffix.len()
            ));
            let last = Client::new(&mut client).try_take().expect("sync fixture");
            assert_eq!(first.len() > 2 * prefix.len(), copied);
            assert_eq!(last.len() > 2 * suffix.len(), copied);
            let endpoint = try_build(router, LIMITS).expect("sync fixture");
            let mut server = sync::Conn::try_new(&endpoint).expect("sync connection");
            server.ingress().try_bind(budget).expect("retention domain");
            server
                .ingress()
                .ingest_owned(Shared::from(head))
                .expect("the fragmented request head fits ingress storage");
            server.drain_events();
            let ((), start) = Tracker::usage(|| {
                server
                    .ingress()
                    .ingest_owned(Shared::from(first))
                    .expect("the first fragment fits ingress storage");
                server.drain_events();
            });
            if copied {
                assert_eq!(start.0, 1);
                assert_eq!(budget.resident(), prefix.len());
            } else {
                assert_eq!(start, (0, 0));
                assert_eq!(budget.resident(), 0);
            }
            let ((), finish) = Tracker::usage(|| {
                server
                    .ingress()
                    .ingest_owned(Shared::from(last))
                    .expect("the final fragment fits ingress storage");
                server.drain_events();
            });
            assert_eq!(finish.0, 1 + usize::from(copied));
            assert_eq!(budget.resident(), 0);
            let drained = Drained::try_for(&mut server).expect("H2 response capture");
            assert!(drained.retained.is_empty());
            assert_eq!(
                drained.try_all().expect("H2 response observation"),
                [prefix, suffix].concat()
            );
        });
    }
}

#[test]
fn second_owner_rejected() {
    with_domain(|domain, proof| {
        let state = State::new(storage::Shared::new());
        let router = Router::new(&state);
        let mut client = client();
        let stream = client
            .streams()
            .headers()
            .request(
                Client::post_head(
                    field::literal!(Value::try_bytes(b"/echo")),
                    field::literal!(Value::try_bytes(b"2")),
                ),
                Boundary::More,
            )
            .expect("the two-chunk request fits the client stream table");
        let head = Client::new(&mut client).try_take().expect("sync fixture");
        assert!(matches!(
            client
                .streams()
                .try_send(stream, b"a", Boundary::More)
                .expect("the client window admits the first owner"),
            DataOutcome::Sent(sent) if sent.get() == 1
        ));
        let first = Client::new(&mut client).try_take().expect("sync fixture");
        assert!(matches!(
            client
                .streams()
                .try_send(stream, b"b", Boundary::End)
                .expect("the client window admits the second owner"),
            DataOutcome::Sent(sent) if sent.get() == 1
        ));
        let second = Client::new(&mut client).try_take().expect("sync fixture");
        let limits = server::Config {
            max_request_body_chunks: 1,
            max_connection_body_chunks: 2,
            ..LIMITS
        };
        let endpoint = try_build(router, limits).expect("sync fixture");
        let mut server = sync::Conn::try_new(&endpoint).expect("sync connection");
        let budget = domain
            .try_budget(limits.max_connection_body_bytes, proof)
            .expect("connection body budget");
        server.ingress().try_bind(budget).expect("retention domain");

        for wire in [head, first] {
            server
                .ingress()
                .ingest_owned(Shared::from(wire))
                .expect("the first retained owner fits ingress credit");
            server.drain_events();
        }
        assert!(server.status().has_stream(stream));
        server
            .ingress()
            .ingest_owned(Shared::from(second))
            .expect("the second owner frame fits ingress storage");
        server.drain_events();

        assert!(!server.status().has_stream(stream));
        assert_eq!(
            state.request_body.get(),
            0,
            "rejected request must not dispatch"
        );
    });
}

#[test]
fn reset_returns_credit() {
    with_domain(|domain, proof| {
        let state = State::new(storage::Shared::new());
        let router = Router::new(&state);
        let mut client = client();
        let key = client
            .streams()
            .headers()
            .request_key(
                Client::post_head(
                    field::literal!(Value::try_bytes(b"/echo")),
                    field::literal!(Value::try_bytes(b"2")),
                ),
                Boundary::More,
            )
            .expect("the reset request fits the client stream table");
        let stream = key.id();
        let head = Client::new(&mut client).try_take().expect("sync fixture");
        assert!(matches!(
            client
                .streams()
                .try_send(stream, b"a", Boundary::More)
                .expect("the client window admits the retained request byte"),
            DataOutcome::Sent(sent) if sent.get() == 1
        ));
        let data = Client::new(&mut client).try_take().expect("sync fixture");
        let limits = server::Config {
            max_request_body_chunks: 2,
            max_connection_body_chunks: 1,
            ..LIMITS
        };
        let endpoint = try_build(router, limits).expect("sync fixture");
        let mut server = sync::Conn::try_new(&endpoint).expect("sync connection");
        let budget = domain
            .try_budget(limits.max_connection_body_bytes, proof)
            .expect("connection body budget");
        server.ingress().try_bind(budget).expect("retention domain");
        for wire in [head, data] {
            server
                .ingress()
                .ingest_owned(Shared::from(wire))
                .expect("the reset candidate fits connection chunk credit");
            server.drain_events();
        }

        client.streams().cancel(key, ErrorCode::Cancel);
        let reset = Client::new(&mut client).try_take().expect("sync fixture");
        server
            .ingress()
            .ingest_owned(Shared::from(reset))
            .expect("the reset frame fits ingress storage");
        server.drain_events();

        let next = Client::new(&mut client)
            .try_post(b"/echo", b"b")
            .expect("sync fixture");
        server
            .ingress()
            .ingest_owned(Shared::from(next))
            .expect("the replacement request reuses released chunk credit");
        server.drain_events();
        assert_ne!(state.request_body.get(), 0);
        let drained = Drained::try_for(&mut server).expect("H2 response capture");
        assert!(drained.retained.is_empty());
        assert_eq!(drained.try_all().expect("H2 response observation"), b"b");
    });
}
