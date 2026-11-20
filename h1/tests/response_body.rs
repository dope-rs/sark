pub mod common;

use std::{cell::Cell, str};

use common::{
    body::{Counted, TEMPLATE},
    fields::{self, DATE, GZIP_POLICY, GZIP_RESPONSE, IDENTITY},
};
use dope_test::alloc::Tracker;
use o3::buffer::write::Slice;
use sark_h1::wire::{head, output};
use sark_http::{
    owner::handoff,
    payload::{
        compress::{Compression, Gzip},
        response::headers::head::Template,
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn preencoded_inline() {
    let fields = [IDENTITY];
    let template = Template::new(&fields);
    let encodes = Cell::new(0);
    let response = Counted::exact(&template, b"encoded", &encodes);
    let mut bytes = [0; 512];
    let mut writer = Slice::new(&mut bytes);
    let mut gzip = Gzip::try_new().expect("test gzip allocation");
    let (outcome, allocs) = Tracker::count(|| {
        output::Egress::new(writer.reborrow(), DATE, GZIP_RESPONSE).route(
            response,
            handoff::Slot::ready(None),
            &mut gzip,
            GZIP_POLICY,
        )
    });
    let output::Outcome::SendCopied { copy, .. } = outcome else {
        panic!("preencoded response must keep its inline body");
    };
    let written = writer.len();
    assert_eq!(allocs, 0);
    assert_eq!(encodes.get(), 1);
    assert_eq!(&bytes[written - copy.written..written], b"encoded");
}

#[test]
fn plain_compressor() {
    let encodes = Cell::new(0);
    let mut bytes = [0; 512];
    let mut writer = Slice::new(&mut bytes);
    let (outcome, allocs) = Tracker::count(|| {
        <() as Compression>::try_new().expect("plain policy");
        let response = Counted::exact(&TEMPLATE, b"plain", &encodes);
        output::Egress::new(writer.reborrow(), DATE, GZIP_RESPONSE).route(
            response,
            handoff::Slot::ready(None),
            &mut (),
            GZIP_POLICY,
        )
    });
    let output::Outcome::SendCopied { copy, .. } = outcome else {
        panic!("plain compression must keep its inline body");
    };
    let written = writer.len();
    assert_eq!(allocs, 0);
    assert_eq!(encodes.get(), 1);
    assert_eq!(&bytes[written - copy.written..written], b"plain");
}

#[test]
fn reuses_inline_on_limit() {
    let encodes = Cell::new(0);
    let body = vec![b'x'; 300 * 1024];
    let response = Counted::exact(&TEMPLATE, &body, &encodes);
    let date = b"Mon, 01 Jan 2026 00:00:00 GMT";
    let mut bytes = vec![0; body.len() + 1024];
    let mut writer = Slice::new(&mut bytes);
    let result = Gzip::try_new();
    let mut gzip = result.expect("test gzip pool allocation");
    let outcome = output::Egress::new(writer.reborrow(), date, GZIP_RESPONSE).route(
        response,
        handoff::Slot::ready(None),
        &mut gzip,
        GZIP_POLICY,
    );
    let output::Outcome::SendCopied { copy, .. } = outcome else {
        panic!("unavailable gzip must keep the encoded inline body");
    };
    let written = writer.len();
    assert_eq!(encodes.get(), 1);
    assert_eq!(copy.written, body.len());
    assert_eq!(&bytes[written - copy.written..written], body.as_slice());
}

#[test]
fn encodes_bounded_once() {
    let encodes = Cell::new(0);
    let body = [b'x'; 512];
    let response = Counted::bounded(&body, &encodes);
    let date = b"Mon, 01 Jan 2026 00:00:00 GMT";
    let mut bytes = [0; 128];
    let mut writer = Slice::new(&mut bytes);
    let outcome = output::Egress::new(writer.reborrow(), date, fields::RESPONSE)
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
    let response = Counted::bounded(&body, &encodes);
    let mut bytes = vec![0; body.len() * 2 + 1024];
    let mut writer = Slice::new(&mut bytes);
    let mut gzip = Gzip::try_new().expect("test gzip pool allocation");
    let outcome = output::Egress::new(writer.reborrow(), DATE, GZIP_RESPONSE).route(
        response,
        handoff::Slot::ready(None),
        &mut gzip,
        GZIP_POLICY,
    );
    let (written, len) = match outcome {
        output::Outcome::SendCopied { copy, .. } => Some((writer.len(), copy.written)),
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
    let response = Counted::bounded(&body, &encodes);
    let date = b"Mon, 01 Jan 2026 00:00:00 GMT";
    let mut bytes = [0; 2048];
    let mut writer = Slice::new(&mut bytes);
    let mut gzip = Gzip::try_new().expect("test gzip pool allocation");
    let outcome = output::Egress::new(writer.reborrow(), date, GZIP_RESPONSE).route(
        response,
        handoff::Slot::ready(None),
        &mut gzip,
        GZIP_POLICY,
    );
    let (head_len, zipped, read) = match outcome {
        output::Outcome::SendCompressed { body, read, .. } => Some((writer.len(), body, read)),
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
