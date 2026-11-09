pub mod common;
use std::{cell::Cell, mem};

use common::response::{__SarkHandlerBorrowedX626F72726F776564 as Borrowed, *};
use dope_test::alloc::Tracker;
use sark::http::{
    field::{self, Value},
    o3::buffer::{storage::Shared, write::Staged},
    payload::response::{dispatch::Complete as _, exact, headers::head::Head, status::Code},
};
use sark_json::Text;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn writes_borrowed_body() {
    let key = field::literal!(Value::try_bytes(b"alpha"));
    let nonce = &b"42"[..];
    let response = RawReply {
        status: Code::OK,
        body: RawBody {
            key: key.get(),
            nonce,
        },
        key,
        owned: Value::new(Shared::try_copy(b"owned").expect("owned header")).expect("valid header"),
    }
    .into_fixed();
    assert!(
        mem::size_of_val(&response) < mem::size_of::<exact::Direct<Head<'_, 2>, RawBody<'_>>>()
    );
    let mut out = [0u8; 64];
    let expected =
        b"content-type: text/plain\r\ncache-control: no-store\r\nx-key: alpha\r\nx-owned: owned\r\n";
    let mut wire = Vec::with_capacity(expected.len());
    let mut fields = Fields {
        out: Staged::try_new(&mut wire, expected.len()).expect("admitted fields"),
        result: Ok(()),
    };

    let (written, (allocs, _)) =
        Tracker::usage(|| response.complete(SliceSink(&mut out, Some(&mut fields))));
    assert_eq!(allocs, 0);
    assert_eq!(written.expect("encoded body"), (Code::OK, 8));
    assert_eq!(&out[..8], b"alpha:42");
    fields.out.try_complete().expect("exact headers").commit();
    assert_eq!(wire, expected);
}

#[test]
fn writes_json_no_alloc() {
    let shared = Text::try_copy("shared").expect("owned JSON fixture");
    let local = Text::try_copy("shared").expect("local JSON fixture");
    let response = Reply {
        status: Code::OK,
        body: Body {
            ok: true,
            value: 42,
            shared,
            local,
        },
    }
    .into_fixed();
    let mut out = [0u8; 128];

    let (written, (allocs, _)) = Tracker::usage(|| response.complete(SliceSink(&mut out, None)));
    let (status, written) = written.expect("json body");
    assert_eq!(status, Code::OK);
    assert_eq!(allocs, 0);
    assert_eq!(
        &out[..written],
        br#"{"ok":true,"value":42,"shared":"shared","local":"shared"}"#
    );
}

#[test]
fn writes_unsigned_no_alloc() {
    for value in [0, usize::MAX] {
        let expected = value.to_string();
        let mut out = [0u8; 20];
        let (written, (allocs, _)) = Tracker::usage(|| {
            Unsigned {
                status: Code::OK,
                body: value,
            }
            .into_fixed()
            .complete(SliceSink(&mut out, None))
        });
        let (status, written) = written.expect("unsigned json body");
        assert_eq!(status, Code::OK);
        assert_eq!(allocs, 0);
        assert_eq!(&out[..written], expected.as_bytes());
    }
}

#[test]
fn visits_length_once() {
    let visits = Cell::new(0);
    let response: <Borrowed as sark::core::manifold::Route<Cell<&Cell<usize>>>>::Response<
        'static,
        'static,
    > = CountedReply {
        status: Code::OK,
        body: Counted(&visits),
    }
    .into_fixed();
    assert_eq!(visits.get(), 0);

    let mut out = [0u8; 16];
    let (status, written) = response
        .complete(SliceSink(&mut out, None))
        .expect("counted body");
    assert_eq!(status, Code::OK);
    assert_eq!(&out[..written], b"{}");
    assert_eq!(visits.get(), 1);
}
