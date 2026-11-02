pub mod common;
use std::{cell::Cell, pin::Pin};

use common::{
    Repeater,
    capacity::try_start,
    headers::Generated,
    protocol::{conn::Controls, session::Frame},
    recipe::Lexical,
    retain::Frames,
};
use dope::core::driver::settings;
use dope_test::alloc::Tracker;
use o3::{
    base::cell::region,
    buffer::{self, hold::scope, pool::Pool, resident, storage},
};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        DataOutcome,
        config::{Config, Validated, capacity},
        event::Event,
        machine::Connection,
    },
    frame::{Ack, numbers::ErrorCode},
    server::{
        self,
        service::Route,
        sync::{Conn, Endpoint},
    },
};
use sark_http::field::metadata::section::rules;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn body_reuse_is_stable() {
    let large = storage::Owned::try_filled(1 << 20, b'x')
        .expect("the shared response fixture must fit")
        .freeze();
    let reusable = large.try_clone().expect("one shared response owner");

    let handler = Repeater(reusable);
    let (reply, usage) = Tracker::usage(|| handler.try_reply());
    let reply = (reply).expect("one reply must fit the reference domain");
    assert_eq!(usage, (0, 0));
    assert_eq!(reply.body().as_slice().as_ptr(), large.as_slice().as_ptr());

    let limits = server::Config {
        max_request_body_bytes: 0,
        max_request_body_chunks: 0,
        max_connection_body_bytes: 0,
        max_connection_body_chunks: 0,
        max_outbound_bytes: 64 << 10,
    };
    let receive = settings::Receive::fixed::<1024, { 64 << 10 }>();
    let plan =
        (server::Plan::new(limits, receive)).expect("the fixed limits must fit a server plan");
    let (endpoint, usage) = Tracker::usage(|| Endpoint::try_new(Route::new(handler), plan));
    let endpoint = (endpoint).expect("the fixed route and plan must fit an endpoint");
    assert_eq!(usage, (0, 0));
    let (conn, usage) = Tracker::usage(|| Conn::try_new(&endpoint));
    (conn).as_ref().expect("the endpoint must fit a connection");
    assert!(
        usage.1 < 512 << 10,
        "server connection allocated {} bytes",
        usage.1
    );
}

#[test]
fn pending_body_is_released() {
    for share in [false, true] {
        for reset in [false, true] {
            let mut domain = resident::Domain::new();
            region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
                let calls = Cell::new(0);
                let limits = server::Config {
                    max_request_body_bytes: 4,
                    max_request_body_chunks: 1,
                    max_connection_body_bytes: 4,
                    max_connection_body_chunks: 1,
                    max_outbound_bytes: 4096,
                };
                let plan = server::Plan::new(limits, settings::Receive::fixed::<2, 4096>())
                    .expect("bounded body plan");
                let budget = domain
                    .get_mut()
                    .try_budget(plan.resident_len(), token.proof())
                    .expect("ingress capacity");
                let endpoint =
                    Endpoint::try_new(Route::new(Lexical(&calls)), plan).expect("endpoint");
                let mut server = Conn::try_new(&endpoint).expect("server connection");
                server.ingress().try_bind(budget).expect("ingress budget");
                let mut client = Connection::<ClientRole>::try_new().expect("client connection");
                let stream = client
                    .streams()
                    .headers()
                    .request(Generated(b""), Boundary::More)
                    .expect("unfinished request");
                server
                    .ingress()
                    .ingest(client.output().first())
                    .expect("request headers");
                client.transfer().drain(usize::MAX);
                server.drain_events();
                server.transfer().drain(usize::MAX);

                let sent = client
                    .streams()
                    .try_send(stream, b"body", Boundary::More)
                    .expect("unfinished DATA");
                assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == 4));
                let pool = Pool::try_new(1, if share { 4 } else { 4096 }).expect("one DATA owner");
                let wire = client.output().first();
                let bytes = if share {
                    server.ingress().ingest(&wire[..9]).expect("DATA header");
                    &wire[9..]
                } else {
                    wire
                };
                let data = pool
                    .try_copy(bytes)
                    .expect("DATA fits owner")
                    .expect("owner is vacant");
                let (allocs, _) = Tracker::during(|| {
                    server
                        .ingress()
                        .ingest_owned(resident::Shared::from(data.into_shared()))
                        .expect("retained DATA");
                });
                client.transfer().drain(usize::MAX);
                server.drain_events();
                assert_eq!(allocs, usize::from(!share));
                assert_eq!(pool.available(), usize::from(!share));
                assert_eq!(budget.resident(), if share { 0 } else { 4 });
                assert_eq!(calls.get(), 0);

                if reset {
                    let stream = client
                        .status()
                        .stream_key(stream)
                        .expect("unfinished stream");
                    client
                        .streams()
                        .try_cancel(stream, ErrorCode::Cancel)
                        .expect("reset unfinished stream");
                    server
                        .ingress()
                        .ingest(client.output().first())
                        .expect("peer reset");
                    client.transfer().drain(usize::MAX);
                    server.drain_events();
                    assert_eq!(pool.available(), 1);
                    assert_eq!(budget.resident(), 0);

                    let next = client
                        .streams()
                        .headers()
                        .request(Generated(b""), Boundary::More)
                        .expect("replacement request");
                    if share {
                        server
                            .ingress()
                            .ingest(client.output().first())
                            .expect("replacement headers");
                        client.transfer().drain(usize::MAX);
                        server.drain_events();
                    }
                    let sent = client
                        .streams()
                        .try_send(next, b"body", Boundary::End)
                        .expect("replacement DATA");
                    assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == 4));
                    let wire = client.output().first();
                    let bytes = if share {
                        server
                            .ingress()
                            .ingest(&wire[..9])
                            .expect("replacement DATA header");
                        &wire[9..]
                    } else {
                        wire
                    };
                    let data = pool
                        .try_copy(bytes)
                        .expect("request fits owner")
                        .expect("released owner is reusable");
                    server
                        .ingress()
                        .ingest_owned(resident::Shared::from(data.into_shared()))
                        .expect("replacement ingress");
                    server.drain_events();
                    assert_eq!(calls.get(), 1, "reset must restore the full body quota");
                }
                server.close();
                assert_eq!(pool.available(), 1);
                assert_eq!(budget.resident(), 0);
            });
        }
    }
}

#[test]
fn scoped_retains_payload() {
    for (len, controls) in [(8, false), (12, false), (8192, false), (8, true)] {
        let mut domain = resident::Domain::new();
        region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
            let budget = domain
                .get_mut()
                .try_budget(if controls { 25 } else { len }, token.proof())
                .expect("final payload and control prefix budget");
            let mut client = Connection::<ClientRole>::try_new().expect("client");
            let stream = try_start(&mut client).expect("request");
            let plan = Validated::new(Config {
                payload_capacity: capacity::Payload::new(1, 1).expect("one retained payload"),
                ..Config::default()
            })
            .expect("server config");
            let mut server =
                Connection::<ServerRole, rules::RetainRaw, resident::Shared<'_>>::from_plan(plan)
                    .expect("server");
            server
                .ingress()
                .ingest_owned(resident::Shared::from(
                    storage::Shared::try_copy(client.output().first()).expect("request owner"),
                ))
                .expect("request head");
            while server.ingress().poll_event().is_some() {}
            server.transfer().drain(usize::MAX);

            let record = stream.record(None);
            let payload = &record[9..9 + len];
            let frame = Frame::new(stream.into_u32(), payload);
            let mut wire = Controls::new(Ack::Complete)
                .try_settings(&[])
                .expect("SETTINGS ACK");
            if controls {
                let ping = Controls::new(Ack::Complete)
                    .try_ping(Default::default())
                    .expect("ping fixture");
                wire.extend_from_slice(&ping);
                wire.extend_from_slice(&frame.try_padded(Boundary::More, 2).expect("padded DATA"));
                wire.extend_from_slice(&ping);
            } else {
                wire.extend_from_slice(&frame.try_data(Boundary::More).expect("DATA"));
            }
            let pool = Pool::try_new(1, 16_384).expect("receive owner");
            let owner = pool
                .try_copy(&wire)
                .expect("wire fits owner")
                .expect("receive owner is vacant")
                .into_shared();
            let pointer = owner
                .as_ref()
                .as_ptr()
                .wrapping_add(if controls { 36 } else { 18 });
            let before = (
                server.status().recv_window().value(),
                server
                    .status()
                    .recv_for(stream)
                    .expect("stream window")
                    .value(),
            );
            let mut consumed = 0;
            let (result, allocs) = Tracker::count(|| {
                server
                    .ingress()
                    .borrow_scope(&scope::Input::new(&owner, &budget), &mut consumed)
            });
            result.expect("retain the final DATA range within budget");
            assert_eq!(consumed, wire.len());
            assert!(matches!(
                server.ingress().poll_event(),
                Some(Event::SettingsAck)
            ));
            let shared = len == 8192;
            if controls {
                assert!(allocs <= 3, "two controls and one DATA copy");
                assert!(matches!(
                    server.ingress().poll_event(),
                    Some(Event::Ping {
                        ack: Ack::Complete,
                        ..
                    })
                ));
            } else {
                assert_eq!(allocs, usize::from(!shared));
            }
            let data = match server.ingress().poll_event() {
                Some(Event::Data {
                    data,
                    boundary: Boundary::More,
                    ..
                }) => Some(data),
                _ => None,
            }
            .expect("one retained DATA event");
            assert_eq!(data.contiguous(), Some(payload));
            assert_eq!(
                data.contiguous().expect("contiguous DATA").as_ptr() == pointer,
                shared
            );
            assert_eq!(
                data.segments()
                    .map(buffer::Residency::resident)
                    .sum::<usize>(),
                if shared { 16_384 } else { len }
            );
            assert_eq!(budget.resident(), if shared { 0 } else { len });
            let debit = (len + if controls { 3 } else { 0 }) as i32;
            assert_eq!(
                (
                    server.status().recv_window().value(),
                    server
                        .status()
                        .recv_for(stream)
                        .expect("stream window")
                        .value(),
                ),
                (before.0 - debit, before.1 - debit)
            );
            if controls {
                assert!(matches!(
                    server.ingress().poll_event(),
                    Some(Event::Ping {
                        ack: Ack::Complete,
                        ..
                    })
                ));
            }
            assert!(server.ingress().poll_event().is_none());
            drop(owner);
            assert_eq!(pool.available(), usize::from(!shared));
            drop(data);
            assert_eq!(pool.available(), 1);
            assert_eq!(budget.resident(), 0);
        });
    }
}
