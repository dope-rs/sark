pub mod common;

use std::{cell::Cell, str};

use common::{
    body::{Bounded, COMPRESSIBLE, Counted, TEMPLATE},
    fields::{self, DATE, ENTRIES},
};
use dope_test::alloc::Tracker;
use o3::buffer::storage;
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
    owner::{evidence, handoff},
    payload::{
        compress::{Compression, Gzip, Output},
        fixed::Body,
        response::{
            exact, fixed,
            headers::{self, head::Head, set},
            statics,
            status::Code,
        },
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn borrowed_plain_uses_output_room() {
    let source = Vec::from(b"plain".as_slice());
    let mut gzip = Gzip::try_new().expect("gzip");
    let mut head = 512;
    for inline in [true, false] {
        let mut bytes = [0; 512];
        let (outcome, allocs) = Tracker::count(|| {
            output::Egress::new(&mut bytes[..head], DATE, fields::RESPONSE).route(
                fixed::Direct::new(Code::OK, (), source.as_slice()),
                handoff::Slot::ready(None),
                cache::Cache::empty(),
                &mut gzip,
                Coding::Gzip,
                common::body::replies::POLICY,
            )
        });
        match outcome {
            output::Outcome::SendCopied { written, copy, .. } if inline => {
                assert_eq!(allocs, 0);
                assert!(copy.cause == evidence::Cause::Inline);
                assert_eq!((copy.read, copy.written), (source.len(), source.len()));
                head = written - source.len();
                assert_eq!(&bytes[head..written], source.as_slice());
            }
            output::Outcome::SendOwnedCopy {
                hdr_written,
                body,
                cause,
                ..
            } if !inline => {
                assert_eq!(allocs, 1);
                assert_eq!(hdr_written, head);
                assert!(cause == evidence::Cause::Borrowed);
                assert_eq!(body.as_ref(), source.as_slice());
            }
            _ => panic!("borrowed placement"),
        }
    }
}

#[test]
fn frames_compressed() {
    let payload = COMPRESSIBLE;
    let result = storage::Shared::try_copy(payload);
    let body = result.expect("fixture body fits shared storage");
    let response = fixed::Direct::new(
        Code::OK,
        Head::new(&TEMPLATE, set::Set::from_items([])),
        body,
    );
    let date = b"Mon, 01 Jan 2026 00:00:00 GMT";
    let mut bytes = [0; 1024];
    let result = Gzip::try_new();
    let mut gzip = result.expect("test gzip pool allocation");
    let outcome = output::Egress::new(&mut bytes, date, fields::RESPONSE).route(
        response,
        handoff::Slot::ready(None),
        cache::Cache::empty(),
        &mut gzip,
        Coding::Gzip,
        route::Policy::new(route::Cache::Bypass, Coding::Gzip, Date::Omit, Server::Omit),
    );
    let output::Outcome::SendCompressed {
        hdr_written: head_len,
        body,
        read,
        ..
    } = outcome
    else {
        panic!("gzip policy must retain the compressed owner");
    };
    let result = str::from_utf8(&bytes[..head_len]);
    let head = result.expect("valid H1 head");
    assert_eq!(read, payload.len());
    assert!(head.contains("content-encoding: gzip\r\n"));
    assert!(head.contains("vary: Accept-Encoding\r\n"));
    assert!(head.contains(&format!("Content-Length: {}\r\n", body.len())));
    assert!(!head.contains(&format!("Content-Length: {}\r\n", payload.len())));
    assert!(head.ends_with("\r\n\r\n"));
    assert!(!head.contains("Date:"));
    assert!(!head.contains("Server:"));
}

#[test]
fn fixed_fields_emit_once() {
    let emits = Cell::new(0);
    let response = fixed::Direct::new(
        Code::OK,
        fields::Drifting {
            emissions: &emits,
            first: field::Value::try_bytes(b"first").expect("first"),
            second: field::Value::try_bytes(b"a different second value").expect("second"),
        },
        storage::Shared::try_copy(COMPRESSIBLE).expect("body"),
    );
    let mut bytes = [0; 512];
    let mut gzip = Gzip::try_new().expect("gzip");
    let (outcome, allocs) = Tracker::count(|| {
        output::Egress::new(&mut bytes, DATE, fields::RESPONSE).route(
            response,
            handoff::Slot::ready(None),
            cache::Cache::empty(),
            &mut gzip,
            Coding::Gzip,
            route::Policy::new(
                route::Cache::Bypass,
                Coding::Gzip,
                Date::Include,
                Server::Include,
            ),
        )
    });
    let length = match outcome {
        output::Outcome::SendCompressed {
            hdr_written: len, ..
        } => Some(len),
        _ => None,
    }
    .expect("compressed response");
    assert_eq!(emits.get(), 1);
    assert_eq!(allocs, 0);
    assert!(
        str::from_utf8(&bytes[..length])
            .expect("head")
            .contains("x-drift: first\r\n")
    );
}

#[test]
fn exact_fields_emit_once() {
    for capacity in [256, 2048] {
        let emits = Cell::new(0);
        let encodes = Cell::new(0);
        let response = exact::Direct::new(
            Code::OK,
            fields::Drifting {
                emissions: &emits,
                first: field::Value::try_bytes(b"first").expect("first"),
                second: field::Value::try_bytes(b"a different second value").expect("second"),
            },
            Counted {
                bytes: COMPRESSIBLE,
                encodes: &encodes,
            },
        );
        let mut bytes = [0; 2048];
        let mut gzip = Gzip::try_new().expect("gzip");
        let (outcome, allocs) = Tracker::count(|| {
            output::Egress::new(&mut bytes[..capacity], DATE, fields::RESPONSE).route(
                response,
                handoff::Slot::ready(None),
                cache::Cache::empty(),
                &mut gzip,
                Coding::Gzip,
                route::Policy::new(
                    route::Cache::Bypass,
                    Coding::Gzip,
                    Date::Include,
                    Server::Include,
                ),
            )
        });
        let length = match outcome {
            output::Outcome::SendCompressed {
                hdr_written: len, ..
            } => Some(len),
            _ => None,
        }
        .expect("compressed response");
        assert_eq!(emits.get(), 1);
        assert_eq!(encodes.get(), 1);
        if capacity == bytes.len() {
            assert_eq!(allocs, 0);
        }
        assert!(
            str::from_utf8(&bytes[..length])
                .expect("head")
                .contains("x-drift: first\r\n")
        );
    }
}

#[test]
fn bounded_keeps_fields() {
    let emits = Cell::new(0);
    let encodes = Cell::new(0);
    let response = Bounded {
        fields: fields::Drifting {
            emissions: &emits,
            first: field::Value::try_bytes(b"measured").expect("first"),
            second: field::Value::try_bytes(b"emitted").expect("second"),
        },
        body: Counted {
            bytes: COMPRESSIBLE,
            encodes: &encodes,
        },
    };
    let mut bytes = [0; 256];
    let mut gzip = Gzip::try_new().expect("gzip");
    let outcome = output::Egress::new(&mut bytes, DATE, fields::RESPONSE).route(
        response,
        handoff::Slot::ready(None),
        cache::Cache::empty(),
        &mut gzip,
        Coding::Gzip,
        route::Policy::new(
            route::Cache::Bypass,
            Coding::Gzip,
            Date::Include,
            Server::Include,
        ),
    );
    let (len, body) = match outcome {
        output::Outcome::SendCompressed {
            hdr_written: len,
            body,
            ..
        } => Some((len, body)),
        _ => None,
    }
    .expect("bounded response must compress");
    assert_eq!(emits.get(), 2);
    assert_eq!(encodes.get(), 1);
    let head = str::from_utf8(&bytes[..len]).expect("head");
    assert!(head.contains("x-drift: emitted\r\n"));
    assert!(head.contains("content-encoding: gzip\r\n"));
    assert!(head.contains(&format!("Content-Length: {}\r\n", body.len())));
}

#[test]
fn fixed_scalar_capacity() {
    let mut gzip = Gzip::try_new().expect("gzip fixture");
    let zipped = match gzip.encode(COMPRESSIBLE) {
        Output::Compressed(body) => Some(body),
        Output::Plain => None,
    }
    .expect("compressed fixture");
    let fields = [
        ENTRIES[0],
        ENTRIES[1],
        headers::Entry::new(
            field::literal!(headers::Name::try_bytes(b"content-encoding")),
            field::literal!(field::Value::try_bytes(b"gzip")),
        ),
        headers::Entry::new(
            field::literal!(headers::Name::try_bytes(b"vary")),
            field::literal!(field::Value::try_bytes(b"Accept-Encoding")),
        ),
    ];
    let mut complete = [0xa5; 256];
    let response = statics::Direct::new(Code::OK, fields, Body::new(zipped.as_ref()));
    let head_len = match output::Egress::new(&mut complete, DATE, fields::RESPONSE)
        .plain(response, head::Connection::KeepAlive)
    {
        output::Outcome::SendStatic {
            hdr_written: len, ..
        } => Some(len),
        _ => None,
    }
    .expect("complete gzip head");

    for capacity in 0..=head_len {
        let mut expected = [0xa5; 256];
        let response = statics::Direct::new(Code::OK, ENTRIES, Body::new(COMPRESSIBLE));
        let plain = output::Egress::new(&mut expected[..capacity], DATE, fields::RESPONSE)
            .plain(response, head::Connection::KeepAlive);
        let wanted = match plain {
            output::Outcome::Failed(Failure::Output(error)) => Some(Err(error)),
            output::Outcome::SendStatic { .. } => {
                let response = statics::Direct::new(Code::OK, fields, Body::new(zipped.as_ref()));
                match output::Egress::new(&mut expected[..capacity], DATE, fields::RESPONSE)
                    .plain(response, head::Connection::KeepAlive)
                {
                    output::Outcome::Failed(Failure::Output(error)) => Some(Err(error)),
                    output::Outcome::SendStatic {
                        hdr_written: len, ..
                    } => Some(Ok(len)),
                    _ => None,
                }
            }
            _ => None,
        }
        .expect("scalar response or capacity failure");

        let mut actual = [0xa5; 256];
        let response = fixed::Direct::new(Code::OK, ENTRIES, Body::new(COMPRESSIBLE));
        let outcome = output::Egress::new(&mut actual[..capacity], DATE, fields::RESPONSE).route(
            response,
            handoff::Slot::ready(None),
            cache::Cache::empty(),
            &mut gzip,
            Coding::Gzip,
            route::Policy::new(
                route::Cache::Bypass,
                Coding::Gzip,
                Date::Include,
                Server::Include,
            ),
        );
        let got = match outcome {
            output::Outcome::Failed(Failure::Output(error)) => Some(Err(error)),
            output::Outcome::SendCompressed {
                hdr_written: len, ..
            } => Some(Ok(len)),
            _ => None,
        }
        .expect("compressed response or capacity failure");
        assert_eq!(got, wanted, "capacity {capacity}");
        assert_eq!(actual, expected, "capacity {capacity}");
    }
}
