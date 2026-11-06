pub mod common;

use std::{cell::Cell, str};

use common::{
    body::{Counted, TEMPLATE},
    fields::{self, DATE, IDENTITY},
};
use dope_test::alloc::Tracker;
use sark_h1::{
    route,
    wire::{cache, head, output},
};
use sark_http::{
    h1::head::{
        request::Coding,
        response::{Date, Server},
    },
    owner::handoff,
    payload::{
        compress::{Compression, Gzip},
        response::{
            bounded, exact,
            headers::{
                head::{Head, Template},
                set,
            },
            status::Code,
        },
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn preencoded_inline() {
    let fields = [IDENTITY];
    let template = Template::new(&fields);
    let encodes = Cell::new(0);
    let response = exact::Direct::new(
        Code::OK,
        Head::new(&template, set::Set::<0>::new()),
        Counted {
            bytes: b"encoded",
            encodes: &encodes,
        },
    );
    let mut bytes = [0; 512];
    let mut gzip = Gzip::try_new().expect("test gzip allocation");
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
    let output::Outcome::SendCopied { written, copy, .. } = outcome else {
        panic!("preencoded response must keep its inline body");
    };
    assert_eq!(allocs, 0);
    assert_eq!(encodes.get(), 1);
    assert_eq!(&bytes[written - copy.written..written], b"encoded");
}

#[test]
fn plain_compressor() {
    let encodes = Cell::new(0);
    let mut bytes = [0; 512];
    let (outcome, allocs) = Tracker::count(|| {
        <() as Compression>::try_new().expect("plain policy");
        let response = exact::Direct::new(
            Code::OK,
            Head::new(&TEMPLATE, set::Set::<0>::new()),
            Counted {
                bytes: b"plain",
                encodes: &encodes,
            },
        );
        output::Egress::new(&mut bytes, DATE, fields::RESPONSE).route(
            response,
            handoff::Slot::ready(None),
            cache::Cache::empty(),
            &mut (),
            Coding::Gzip,
            route::Policy::new(
                route::Cache::Bypass,
                Coding::Gzip,
                Date::Include,
                Server::Include,
            ),
        )
    });
    let output::Outcome::SendCopied { written, copy, .. } = outcome else {
        panic!("plain compression must keep its inline body");
    };
    assert_eq!(allocs, 0);
    assert_eq!(encodes.get(), 1);
    assert_eq!(&bytes[written - copy.written..written], b"plain");
}

#[test]
fn reuses_inline_on_limit() {
    let encodes = Cell::new(0);
    let body = vec![b'x'; 300 * 1024];
    let response = exact::Direct::new(
        Code::OK,
        Head::new(&TEMPLATE, set::Set::<0>::new()),
        Counted {
            bytes: &body,
            encodes: &encodes,
        },
    );
    let date = b"Mon, 01 Jan 2026 00:00:00 GMT";
    let mut bytes = vec![0; body.len() + 1024];
    let result = Gzip::try_new();
    let mut gzip = result.expect("test gzip pool allocation");
    let outcome = output::Egress::new(&mut bytes, date, fields::RESPONSE).route(
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
    let output::Outcome::SendCopied { written, copy, .. } = outcome else {
        panic!("unavailable gzip must keep the encoded inline body");
    };
    assert_eq!(encodes.get(), 1);
    assert_eq!(copy.written, body.len());
    assert_eq!(&bytes[written - copy.written..written], body.as_slice());
}

#[test]
fn encodes_bounded_once() {
    let encodes = Cell::new(0);
    let body = [b'x'; 512];
    let response: bounded::Output<'_, _, 0> = bounded::Output::new(
        Code::OK,
        &TEMPLATE,
        set::Set::new(),
        Counted {
            bytes: &body,
            encodes: &encodes,
        },
    );
    let date = b"Mon, 01 Jan 2026 00:00:00 GMT";
    let mut bytes = [0; 128];
    let outcome = output::Egress::new(&mut bytes, date, fields::RESPONSE)
        .plain(response, head::Connection::KeepAlive);
    let output::Outcome::SendShared { body: retained, .. } = outcome else {
        panic!("bounded body must retain its one materialization");
    };
    assert_eq!(encodes.get(), 1);
    assert_eq!(retained.as_slice(), body);
}

#[test]
fn bounded_reuses_inline() {
    let encodes = Cell::new(0);
    let body = vec![b'x'; 300 * 1024];
    let response: bounded::Output<'_, _, 0> = bounded::Output::new(
        Code::OK,
        &TEMPLATE,
        set::Set::new(),
        Counted {
            bytes: &body,
            encodes: &encodes,
        },
    );
    let mut bytes = vec![0; body.len() * 2 + 1024];
    let mut gzip = Gzip::try_new().expect("test gzip pool allocation");
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
    let (written, len) = match outcome {
        output::Outcome::SendCopied { written, copy, .. } => Some((written, copy.written)),
        _ => None,
    }
    .expect("bounded fallback keeps the inline body");
    assert_eq!(encodes.get(), 1);
    assert_eq!(len, body.len());
    assert_eq!(&bytes[written - len..written], body.as_slice());
}

#[test]
fn compresses_bounded_once() {
    let encodes = Cell::new(0);
    let body = [b'x'; 512];
    let response: bounded::Output<'_, _, 0> = bounded::Output::new(
        Code::OK,
        &TEMPLATE,
        set::Set::new(),
        Counted {
            bytes: &body,
            encodes: &encodes,
        },
    );
    let date = b"Mon, 01 Jan 2026 00:00:00 GMT";
    let mut bytes = [0; 2048];
    let mut gzip = Gzip::try_new().expect("test gzip pool allocation");
    let outcome = output::Egress::new(&mut bytes, date, fields::RESPONSE).route(
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
    let (head_len, zipped, read) = match outcome {
        output::Outcome::SendCompressed {
            hdr_written: head,
            body,
            read,
            ..
        } => Some((head, body, read)),
        _ => None,
    }
    .expect("bounded response must compress");
    assert_eq!(encodes.get(), 1);
    assert_eq!(read, body.len());
    let head = str::from_utf8(&bytes[..head_len]).expect("valid H1 head");
    assert!(head.contains("content-encoding: gzip\r\n"));
    assert!(head.contains(&format!("Content-Length: {}\r\n", zipped.len())));
    assert!(head.ends_with("\r\n\r\n"));
}
