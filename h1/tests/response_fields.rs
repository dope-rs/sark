pub mod common;

use std::cell::Cell;

use common::fields::{
    Counted, DATE, DRIFT_VALUES, Drifting, ENCODED_BODY, ENTRIES, GENERATED_IDENTITY, GROUP,
    GROUP_ENCODING, IDENTITY, RESPONSE, try_encode,
};
use sark_h1::{
    route,
    wire::{Failure, cache, head, output},
};
use sark_http::{
    field,
    h1::head::{
        request::Coding,
        response::{Date, Server},
    },
    owner::handoff,
    payload::{
        compress::{Compression as _, Gzip},
        fixed::Body,
        response::{headers, statics, status::Code},
    },
};

#[test]
fn head_emits_once() {
    let emits = Cell::new(0);
    let response =
        statics::Direct::new(Code::OK, Counted { emissions: &emits }, Body::new(b"body"));
    let mut bytes = [0; 512];
    let outcome = output::Egress::new(&mut bytes, DATE, RESPONSE)
        .plain(response, head::Connection::KeepAlive);
    let output::Outcome::SendStatic {
        hdr_written: head_len,
        ..
    } = outcome
    else {
        panic!("valid static response must be emitted");
    };

    assert_eq!(emits.get(), 1);
    assert!(
        bytes[..head_len]
            .windows(b"x-actual: present\r\n".len())
            .any(|window| window == b"x-actual: present\r\n")
    );
}

#[test]
fn grouped_capacity() {
    let mut expected = [0xa5; 256];
    let response = statics::Direct::new(Code::OK, ENTRIES, Body::new(b"body"));
    let length = match output::Egress::new(&mut expected, DATE, RESPONSE)
        .plain(response, head::Connection::KeepAlive)
    {
        output::Outcome::SendStatic {
            hdr_written: len, ..
        } => Some(len),
        _ => None,
    }
    .expect("the complete scalar header must fit");

    for capacity in 0..=length {
        let mut scalar = [0xa5; 256];
        let mut grouped = scalar;
        let response = statics::Direct::new(Code::OK, ENTRIES, Body::new(b"body"));
        let first = output::Egress::new(&mut scalar[..capacity], DATE, RESPONSE)
            .plain(response, head::Connection::KeepAlive);
        let response = statics::Direct::new(Code::OK, GROUP, Body::new(b"body"));
        let second = output::Egress::new(&mut grouped[..capacity], DATE, RESPONSE)
            .plain(response, head::Connection::KeepAlive);
        let results = match (first, second) {
            (
                output::Outcome::Failed(Failure::Output(scalar)),
                output::Outcome::Failed(Failure::Output(grouped)),
            ) => Some((Err(scalar), Err(grouped))),
            (
                output::Outcome::SendStatic {
                    hdr_written: scalar,
                    ..
                },
                output::Outcome::SendStatic {
                    hdr_written: grouped,
                    ..
                },
            ) => Some((Ok(scalar), Ok(grouped))),
            _ => None,
        };
        let (left, right) = results.expect("matching output variants");
        assert_eq!(right, left, "capacity {capacity}");
        assert_eq!(right.is_ok(), capacity == length);
        assert_eq!(grouped, scalar, "capacity {capacity}");
    }
}

#[test]
fn cache_rejects_drift() {
    for &(first, second) in DRIFT_VALUES {
        let emits = Cell::new(0);
        let response = statics::Direct::new(
            Code::OK,
            Drifting {
                emissions: &emits,
                first: field::Value::try_bytes(first).expect("first"),
                second: field::Value::try_bytes(second).expect("second"),
            },
            Body::new(b"body"),
        );
        let mut bytes = [0; 512];
        let result = Gzip::try_new();
        let mut gzip = result.expect("test gzip allocation");
        let outcome = output::Egress::new(&mut bytes, DATE, RESPONSE).route(
            response,
            handoff::Slot::ready(None),
            cache::Cache::empty(),
            &mut gzip,
            Coding::Plain,
            route::Policy::new(
                route::Cache::Store,
                Coding::Plain,
                Date::Include,
                Server::Include,
            ),
        );

        assert!(matches!(
            outcome,
            output::Outcome::Failed(Failure::Output(_))
        ));
        assert_eq!(emits.get(), 2);
    }
}

#[test]
fn encoding_preserves_body() {
    preserve([IDENTITY]);
    preserve(GROUP_ENCODING);
    preserve(GENERATED_IDENTITY);
}

fn preserve(source: impl headers::Source) {
    let input = ENCODED_BODY;
    let outcome = try_encode(source, &input).expect("compression");
    let output::Outcome::SendStatic { body, .. } = outcome else {
        panic!("an existing encoding must preserve the original body");
    };
    assert_eq!(body.as_slice(), input);
    assert_eq!(body.as_slice().as_ptr(), input.as_ptr());
}
