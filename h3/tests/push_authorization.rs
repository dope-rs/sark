use std::cell::Cell;

use crate::common::{
    conn::Events,
    field::{Switching, entry},
    stream::Stream as _,
};

pub mod common;

use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_h3::conn::{
    Connection, Error,
    event::{Event, Role},
    io::{Receive, Send},
    options, push,
};
use sark_http::field::literal;

#[test]
fn push_grant_commit() {
    let mut conn = Connection::with_role(Role::Client).expect("connection");
    assert!(matches!(
        Send::new(&mut conn).max_push(1),
        Err(Error::ControlMissing)
    ));
    assert!(matches!(
        Send::new(&mut conn).cancel(0),
        Err(Error::Push(push::Error::Unpermitted { maximum: None, .. }))
    ));
    conn.allow_push(Role::Client, 1).expect("grant");
    Send::new(&mut conn)
        .cancel(1)
        .expect("cancel before promise");
    assert!(matches!(
        Send::new(&mut conn).max_push(0),
        Err(Error::PushIdOrder {
            previous: 1,
            actual: 0
        })
    ));
    assert!(matches!(
        Send::new(&mut conn).cancel(2),
        Err(Error::Push(push::Error::Unpermitted {
            maximum: Some(1),
            ..
        }))
    ));
}

#[test]
fn push_history() {
    let mut config = options::Config::default();
    config.streams.capacity = 1;
    let mut conn = Connection::with_config(Role::Server, config).expect("connection");
    conn.allow_push(Role::Server, 2).expect("grant");
    conn.control()
        .open(Id::new(3).expect("control"))
        .expect("control");
    assert!(matches!(
        Send::new(&mut conn).cancel(0),
        Err(Error::Push(push::Error::Unknown { push_id: 0 }))
    ));
    conn.promise(0).expect("promise");
    while conn.events().poll_write().expect("write").is_some() {}
    Send::new(&mut conn).cancel(0).expect("retained identity");
    conn.promise(0).expect("duplicate identity");
    let before = conn.wire_len();
    assert!(matches!(
        conn.promise(1),
        Err(Error::Push(push::Error::Capacity {
            push_id: 1,
            capacity: 1
        }))
    ));
    assert_eq!(conn.wire_len(), before);
    assert!(matches!(
        Send::new(&mut conn).cancel(1),
        Err(Error::Push(push::Error::Unknown { push_id: 1 }))
    ));
}

#[test]
fn promise_mismatch_atomic() {
    let mut conn = Connection::with_role(Role::Server).expect("connection");
    conn.allow_push(Role::Server, 0).expect("grant");
    let mut fields = [
        literal!(entry(b":method", b"GET")),
        literal!(entry(b":scheme", b"https")),
        literal!(entry(b":path", b"/asset")),
        literal!(entry(b":authority", b"example.test")),
        literal!(entry(b"x-tag", b"original")),
    ];
    Send::new(&mut conn)
        .promise(Id::ZERO, 0, fields)
        .expect("promise");
    let before = conn.wire_len();
    fields[4] = literal!(entry(b"x-tag", b"changed"));
    assert!(matches!(
        Send::new(&mut conn).promise(Id::new(4).expect("request stream"), 0, fields),
        Err(Error::Push(push::Error::Mismatch { push_id: 0 }))
    ));
    assert_eq!(conn.wire_len(), before);
    fields[4] = literal!(entry(b"x-tag", b"original"));
    Send::new(&mut conn)
        .promise(Id::new(4).expect("request stream"), 0, fields)
        .expect("original history survives mismatch");
}

#[test]
fn promise_source_once() {
    let mut conn = Connection::with_role(Role::Server).expect("connection");
    conn.allow_push(Role::Server, 0).expect("grant");
    let first = [
        literal!(entry(b":method", b"GET")),
        literal!(entry(b":scheme", b"https")),
        literal!(entry(b":path", b"/asset")),
        literal!(entry(b":authority", b"example.test")),
        literal!(entry(b"x-tag", b"original")),
    ];
    let mut next = first;
    next[4] = literal!(entry(b"x-tag", b"changed"));
    let source = Switching {
        calls: Cell::new(0),
        first,
        next,
    };
    Send::new(&mut conn)
        .promise(Id::ZERO, 0, &source)
        .expect("promise");
    assert_eq!(source.calls.get(), 1);
    Send::new(&mut conn)
        .promise(Id::new(4).expect("request stream"), 0, first)
        .expect("history matches emitted fields");
    let before = conn.wire_len();
    assert!(matches!(
        Send::new(&mut conn).promise(Id::new(8).expect("request stream"), 0, next),
        Err(Error::Push(push::Error::Mismatch { push_id: 0 }))
    ));
    assert_eq!(conn.wire_len(), before);
}

#[test]
fn goaway_stops_new_pushes() {
    let mut conn = Connection::with_role(Role::Server).expect("connection");
    conn.allow_push(Role::Server, 2).expect("grant");
    conn.promise(0).expect("initial promise");
    Receive::new(&mut conn)
        .borrowed(
            Id::new(2).expect("peer control"),
            &[7, 1, 2],
            Boundary::More,
        )
        .expect("peer push cutoff");
    assert!(matches!(
        Events::new(&mut conn).poll_token().expect("goaway event"),
        Some(Event::GoAway { id: 2 })
    ));
    for push_id in [1, 2] {
        let before = conn.wire_len();
        assert!(matches!(
            conn.promise(push_id),
            Err(Error::Push(push::Error::GoAway { push_id: actual, cutoff: 2 }))
                if actual == push_id
        ));
        assert_eq!(conn.wire_len(), before);
    }
    conn.promise(0).expect("retained promise below cutoff");
}

#[test]
fn promise_history_bound() {
    let mut config = options::Config::default();
    config.egress.resident_bytes = 512;
    let mut conn = Connection::with_config(Role::Server, config).expect("connection");
    conn.allow_push(Role::Server, 2).expect("grant");
    for id in [0, 1] {
        conn.promise(id).expect("retained promise");
        while conn.events().poll_write().expect("write").is_some() {}
    }
    let before = conn.wire_len();
    assert!(matches!(
        conn.promise(2),
        Err(Error::Push(push::Error::Storage { retained, additional, limit: 512 }))
            if retained <= 512 && retained + additional > 512
    ));
    assert_eq!(conn.wire_len(), before);
    assert!(matches!(
        Send::new(&mut conn).cancel(2),
        Err(Error::Push(push::Error::Unknown { push_id: 2 }))
    ));
    conn.promise(0)
        .expect("existing history survives storage rejection");
}
