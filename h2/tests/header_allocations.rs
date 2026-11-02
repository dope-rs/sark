use common::{
    headers::{Frames, Response},
    protocol::Session,
};
use sark_h2::conn::event::Event;

pub mod common;

use std::pin::Pin;

use dope_test::alloc::Tracker;
use o3::{base::cell::region, buffer::resident};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        config::{Config, Validated},
        machine::Connection,
    },
};
use sark_http::field::metadata::section::rules;

use crate::common::headers::{Authority, Generated};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn writes_generated() {
    let mut client = Connection::<ClientRole>::try_new().expect("client allocation");
    client
        .streams()
        .headers()
        .request(Generated(b"direct%value"), Boundary::End)
        .expect("matching generated length");

    let mut server = Connection::<ServerRole>::try_new().expect("server allocation");
    server
        .ingress()
        .ingest(client.output().first())
        .expect("generated request frame");
    let headers = loop {
        let event = server.ingress().poll_event().expect("request event");
        if let Event::Headers { head, .. } = event {
            let (headers, _, _) = head.into_parts();
            break headers;
        }
    };
    assert_eq!(
        headers.get(b"x-generated"),
        Some(b"ZGlyZWN0JXZhbHVl".as_slice())
    );
}

#[test]
fn fragment_has_no_alloc() {
    let mut conn = Connection::<ServerRole, rules::Discard>::from_plan(
        Validated::new(Config::default()).expect("default H2 configuration"),
    )
    .expect("server allocation");
    Session::new(&mut conn)
        .try_prime()
        .expect("client preface and settings");

    let (first, second) = Frames::try_new(1)
        .expect("fixture stream")
        .try_split()
        .expect("header fixture construction");
    conn.ingress().ingest_owned(first).expect("first fragment");
    conn.ingress()
        .ingest_owned(second)
        .expect("second fragment");
    Response::new(&mut conn)
        .try_finish()
        .ok()
        .expect("request must accept a terminal response");

    let (first, second) = Frames::try_new(3)
        .expect("fixture stream")
        .try_split()
        .expect("header fixture construction");
    let (count, _) = Tracker::during(|| {
        conn.ingress().ingest_owned(first).expect("first fragment");
        conn.ingress()
            .ingest_owned(second)
            .expect("second fragment");
        Response::new(&mut conn)
            .try_finish()
            .ok()
            .expect("request must accept a terminal response");
    });
    assert_eq!(count, 0);
}

#[test]
fn atomic_has_no_alloc() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(Config::default().inbound_capacity, token.proof())
            .expect("ingress capacity");
        let mut conn = Connection::<ServerRole, rules::Discard, resident::Shared<'_>>::from_plan(
            Validated::new(Config::default()).expect("default H2 configuration"),
        )
        .expect("shared server allocation");
        conn.ingress().try_bind(budget).expect("ingress budget");
        Session::new(&mut conn)
            .try_prime()
            .expect("client preface and settings");

        let (first, second) = Frames::try_new(1)
            .expect("fixture stream")
            .try_split()
            .expect("header fixture construction");
        conn.ingress()
            .ingest_owned(resident::Shared::from(first))
            .expect("first fragment");
        conn.ingress()
            .ingest_owned(resident::Shared::from(second))
            .expect("second fragment");
        assert!(
            Session::new(&mut conn)
                .try_finish()
                .expect("atomic response must finish")
                .is_none()
        );

        let (first, second) = Frames::try_new(3)
            .expect("fixture stream")
            .try_split()
            .expect("header fixture construction");
        let (count, _) = Tracker::during(|| {
            conn.ingress()
                .ingest_owned(resident::Shared::from(first))
                .expect("first fragment");
            conn.ingress()
                .ingest_owned(resident::Shared::from(second))
                .expect("second fragment");
            assert!(
                Session::new(&mut conn)
                    .try_finish()
                    .expect("atomic response must finish")
                    .is_none()
            );
        });
        assert_eq!(count, 0);
    });
}

#[test]
fn owners_have_no_alloc() {
    let mut conn = Connection::<ServerRole, rules::Discard>::from_plan(
        Validated::new(Config::default()).expect("default H2 configuration"),
    )
    .expect("server allocation");
    Session::new(&mut conn)
        .try_prime()
        .expect("client preface and settings");

    let frames = Frames::try_new(1)
        .expect("fixture stream")
        .try_owners()
        .expect("header fixture construction");
    assert!(frames.len() > 16 && frames.len() <= 65);
    for frame in frames {
        conn.ingress().ingest_owned(frame).expect("owner frame");
    }
    Response::new(&mut conn)
        .try_finish()
        .ok()
        .expect("request must accept a terminal response");

    let frames = Frames::try_new(3)
        .expect("fixture stream")
        .try_owners()
        .expect("header fixture construction");
    assert!(frames.len() > 16 && frames.len() <= 65);
    let (count, _) = Tracker::during(|| {
        for frame in frames {
            conn.ingress().ingest_owned(frame).expect("owner frame");
        }
        Response::new(&mut conn)
            .try_finish()
            .ok()
            .expect("request must accept a terminal response");
    });
    assert_eq!(count, 0);
}

#[test]
fn borrowed_allocates_two() {
    let mut conn = Connection::<ServerRole, rules::Discard>::from_plan(
        Validated::new(Config::default()).expect("default H2 configuration"),
    )
    .expect("server allocation");
    Session::new(&mut conn)
        .try_prime()
        .expect("client preface and settings");

    let frames = Frames::try_new(1)
        .expect("fixture stream")
        .try_wire()
        .expect("header fixture construction");
    assert!(frames.len() > 16 && frames.len() <= 65);
    for frame in frames {
        conn.ingress().ingest(&frame).expect("borrowed frame");
    }
    Response::new(&mut conn)
        .try_finish()
        .ok()
        .expect("request must accept a terminal response");

    let frames = Frames::try_new(3)
        .expect("fixture stream")
        .try_wire()
        .expect("header fixture construction");
    assert!(frames.len() > 16 && frames.len() <= 65);
    let (count, _) = Tracker::during(|| {
        for frame in &frames {
            conn.ingress().ingest(frame).expect("borrowed frame");
        }
        Response::new(&mut conn)
            .try_finish()
            .ok()
            .expect("request must accept a terminal response");
    });
    assert_eq!(count, 2);
}

#[test]
fn push_has_no_alloc() {
    let mut conn =
        Connection::<ClientRole>::with_push::<Authority>().expect("push client allocation");
    Session::new(&mut conn)
        .try_prime()
        .expect("server settings");

    Session::new(&mut conn)
        .try_start()
        .expect("client request stream must start");
    conn.ingress()
        .ingest_owned(
            Frames::try_new(1)
                .expect("fixture stream")
                .try_push(2)
                .expect("header fixture construction"),
        )
        .expect("warm push promise");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PushPromise { .. })
    ));

    Session::new(&mut conn)
        .try_start()
        .expect("client request stream must start");
    let wire = Frames::try_new(3)
        .expect("fixture stream")
        .try_push(4)
        .expect("header fixture construction");
    let mut accepted = false;
    let (count, _) = Tracker::during(|| {
        conn.ingress()
            .ingest_owned(wire)
            .expect("measured push promise");
        accepted = matches!(conn.ingress().poll_event(), Some(Event::PushPromise { .. }));
    });

    assert_eq!(count, 0);
    assert!(accepted);
}
