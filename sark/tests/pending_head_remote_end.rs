pub mod common;

use std::{cell::Cell, pin::Pin};

use common::{
    drained::Drained,
    sync::{
        Client, LIMITS,
        pending::{Router, State},
    },
};
use dope::core::driver::settings;
use o3::{
    base::cell::region,
    buffer::resident::{self, Shared},
};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{
        DataOutcome,
        config::{self, capacity::Payload},
        machine,
    },
    server::{self, sync},
};
use sark_http::field::{Entry, Pseudo, Value};

#[test]
fn commit_keeps_remote_end() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let state = State {
            replies: Cell::new(0),
            value: Value::new(vec![b'~'; 16_000]).expect("header value"),
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
        let mut pending = None;
        for n in 0..8 {
            let stream = client
                .streams()
                .headers()
                .request(
                    [
                        Entry::pseudo(Pseudo::Method, Value::try_bytes(b"POST").expect("method")),
                        Entry::pseudo(Pseudo::Scheme, Value::try_bytes(b"http").expect("scheme")),
                        Entry::pseudo(Pseudo::Path, Value::try_bytes(b"/pending").expect("path")),
                        Entry::pseudo(
                            Pseudo::Authority,
                            Value::try_bytes(b"localhost").expect("authority"),
                        ),
                    ],
                    Boundary::More,
                )
                .expect("request");
            let wire = Client::new(&mut client).try_take().expect("request wire");
            server
                .ingress()
                .ingest_owned(Shared::from(wire))
                .expect("request ingress");
            server.drain_events();
            assert_eq!(state.replies.get(), n + 1, "response admitted once");
            assert_eq!(
                client
                    .streams()
                    .try_send(stream, b"", Boundary::End)
                    .expect("request end"),
                DataOutcome::Ended,
            );
            let wire = Client::new(&mut client).try_take().expect("end wire");
            server
                .ingress()
                .ingest_owned(Shared::from(wire))
                .expect("end ingress");
            server.drain_events();
            if server.status().has_stream(stream) {
                pending = Some(stream);
                break;
            }
        }
        let pending = pending.expect("egress pressure retains an encoded response");
        Drained::try_for(&mut server).expect("release earlier output");
        server.pump();
        assert!(
            !server.status().has_stream(pending),
            "commit preserves the remote END_STREAM"
        );
    });
}
