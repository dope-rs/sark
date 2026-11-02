pub mod common;

use std::pin::Pin;

use common::{
    capacity::try_start,
    protocol::{Session, conn::Controls},
};
use o3::{
    base::cell::region,
    buffer::{hold::scope, pool::Pool, resident, storage, write::consume::Consume as _},
};
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        Error,
        config::{Config, Validated},
        event::Event,
        machine::Connection,
    },
    frame::{Ack, numbers::ParseError},
};
use sark_http::field::metadata::section::rules;

#[test]
fn scoped_ack_pressure() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(4096, token.proof())
            .expect("receive budget");
        let plan = Validated::new(Config {
            event_capacity: 2,
            ..Config::default()
        })
        .expect("server config");
        let mut server =
            Connection::<ServerRole, rules::RetainRaw, resident::Shared<'_>>::from_plan(plan)
                .expect("server");
        server.ingress().try_bind(budget).expect("bind receive");
        Session::new(&mut server).try_prime().expect("preface");
        let ping = Controls::new(Ack::Complete)
            .try_ping(Default::default())
            .expect("PING ACK");
        for _ in 0..2 {
            server.ingress().ingest(&ping).expect("fill events");
        }
        let mut client = Connection::<ClientRole>::try_new().expect("client");
        client.transfer().drain(usize::MAX);
        let stream = try_start(&mut client).expect("request");
        let mut wire = Controls::new(Ack::Complete)
            .try_settings(&[])
            .expect("SETTINGS ACK");
        wire.extend_from_slice(client.output().first());
        let pool = Pool::try_new(1, 16_384).expect("receive pool");
        let mut owner = pool
            .try_copy(&wire)
            .expect("wire fits receive")
            .expect("receive vacant")
            .into_shared();
        let mut consumed = 0;
        assert!(matches!(
            server
                .ingress()
                .borrow_scope(&scope::Input::new(&owner, &budget), &mut consumed),
            Err(Error::EventFull { used: 2, limit: 2 })
        ));
        assert_eq!(consumed, 0);
        assert!(matches!(
            server.ingress().poll_event(),
            Some(Event::Ping { .. })
        ));
        assert!(matches!(
            server
                .ingress()
                .borrow_scope(&scope::Input::new(&owner, &budget), &mut consumed),
            Err(Error::EventFull { used: 2, limit: 2 })
        ));
        assert_eq!(consumed, 9);
        assert!(matches!(
            server.ingress().poll_event(),
            Some(Event::Ping { .. })
        ));
        assert!(matches!(
            server.ingress().poll_event(),
            Some(Event::SettingsAck)
        ));
        owner.try_consume(consumed).expect("commit ACK prefix");
        server
            .ingress()
            .borrow_scope(&scope::Input::new(&owner, &budget), &mut consumed)
            .expect("retry HEADERS");
        assert_eq!(consumed, wire.len() - 9);
        assert!(
            matches!(server.ingress().poll_event(), Some(Event::Headers { stream_id, .. }) if stream_id.id() == stream)
        );
        assert!(server.ingress().poll_event().is_none());
        drop(owner);
        assert_eq!(pool.available(), 1);
        assert_eq!(budget.resident(), 0);
        for part in wire[..9].chunks(4) {
            let owner = storage::Shared::try_copy(part).expect("ACK fragment");
            server
                .ingress()
                .borrow_scope(&scope::Input::new(&owner, &budget), &mut consumed)
                .expect("retained ACK fragments");
            assert_eq!(consumed, part.len());
        }
        assert!(matches!(
            server.ingress().poll_event(),
            Some(Event::SettingsAck)
        ));
        assert!(server.ingress().poll_event().is_none());
        let mut bad = wire[..9].to_vec();
        bad[2] = 6;
        bad.extend_from_slice(&[0; 6]);
        let owner = storage::Shared::try_copy(&bad).expect("invalid ACK owner");
        assert!(matches!(
            server
                .ingress()
                .borrow_scope(&scope::Input::new(&owner, &budget), &mut consumed),
            Err(Error::ParseError(ParseError::SettingsSize {
                actual: 6,
                ack: Ack::Complete,
            }))
        ));
    });
}
