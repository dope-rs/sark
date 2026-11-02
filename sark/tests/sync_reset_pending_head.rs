pub mod common;

use std::{cell::Cell, pin::Pin};

use common::{
    drained::Drained,
    sync::{Client, LIMITS, Router, State},
};
use dope::core::driver::settings;
use o3::{
    base::cell::region,
    buffer::{
        resident::{self, Shared},
        storage,
    },
};
use sark_h2::{
    ClientRole,
    conn::{
        config::{self, capacity::Payload},
        machine,
    },
    frame::numbers::ErrorCode,
    server::{self, sync},
    stream::Id,
};

#[test]
fn reset_releases_encoder() {
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
        let protocol = config::Config {
            payload_capacity: Payload::new(1, 1).expect("payload slots"),
            ..Default::default()
        };
        let plan = server::Plan::new(LIMITS, settings::Receive::fixed::<2, { 64 << 10 }>())
            .expect("server plan")
            .with_config(protocol);
        let endpoint = sync::Endpoint::try_new(Router::new(&state), plan).expect("endpoint");
        let budget = domain
            .get_mut()
            .try_budget(LIMITS.max_connection_body_bytes, token.proof())
            .expect("resident budget");
        let mut server = sync::Conn::try_new(&endpoint).expect("server");
        server
            .ingress()
            .try_bind(budget)
            .expect("connection retention");
        let mut client = machine::Connection::<ClientRole>::try_new().expect("client");
        let value = vec![b'~'; 16_000];
        let mut pending = None;
        for n in 0..8 {
            let wire = Client::new(&mut client)
                .try_post(b"/headers", &value)
                .expect("request");
            server
                .ingress()
                .ingest_owned(Shared::from(wire))
                .expect("ingress");
            server.drain_events();
            let stream = Id::new(2 * n + 1).expect("stream");
            if server.status().has_stream(stream) {
                assert_eq!(
                    state.dispatches.get(),
                    n as usize + 1,
                    "head was encoded once"
                );
                pending = Some(stream);
                break;
            }
        }
        let pending = pending.expect("retained egress slots must hold an encoded head");
        let key = client.status().stream_key(pending).expect("client stream");
        client.streams().cancel(key, ErrorCode::Cancel);
        let wire = Client::new(&mut client).try_take().expect("reset");
        server
            .ingress()
            .ingest_owned(Shared::from(wire))
            .expect("reset ingress");
        server.drain_events();
        Drained::try_for(&mut server).expect("release output");

        let wire = Client::new(&mut client)
            .try_get(b"/items/survivor")
            .expect("survivor");
        server
            .ingress()
            .ingest_owned(Shared::from(wire))
            .expect("survivor ingress");
        server.drain_events();
        server.pump();
        let output = Drained::try_for(&mut server).expect("response");
        assert_eq!(output.try_all().expect("response data"), b"survivor");
        assert_eq!(state.header_error.get(), None);
    });
}
