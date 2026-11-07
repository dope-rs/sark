pub mod common;

use std::{cell::Cell, num::NonZero};

use common::response::{
    self,
    body::{Checked, Counted, Short},
    headers::{EMPTY, SEMANTIC, STATIC},
    status,
};
use dope_test::alloc::Tracker;
use o3::buffer::{
    resident::bound::{Bound as _, Limit},
    segments,
    storage::{self, Shared},
};
use sark_http::{
    field::{self, Lower, Value},
    payload::{
        fixed::Body,
        owned,
        response::{
            Body as _, bounded,
            dispatch::{self, Immediate},
            exact, fixed,
            headers::{Name, head::Head, set::Set, value::Item},
            statics,
            status::Code,
        },
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn sets_borrow_name_proofs() {
    let raw = field::Name::try_bytes(b"X-Upper").expect("field name");
    let name = Name::new(Lower::try_from(raw).expect("normalized owner")).expect("response name");
    let (item, allocs) =
        Tracker::usage(|| Item::borrow(name.borrow(), field::literal!(Value::try_bytes(b"value"))));
    assert_eq!(allocs, (0, 0));
    assert_eq!(item.name_bytes(), b"x-upper");
    assert_eq!(item.name_bytes().as_ptr(), name.as_bytes().as_ptr());
}

#[test]
fn headers_stay_structured() {
    let raw = field::Name::try_bytes(b"X-Dynamic").expect("field name");
    let name = Name::new(Lower::try_from(raw).expect("normalized owner")).expect("response name");
    let result = Shared::try_copy(b"body");
    let body = result.expect("the response body must allocate");
    let dynamic = Set::from_items([Item::from_str(
        name.borrow(),
        Value::new("value").expect("field value"),
    )]);
    let response = fixed::Direct::new(Code::CREATED, Head::new(&STATIC, dynamic), body);

    let capture = response::Capture::try_from(response).expect("response encoding");
    assert_eq!(capture.status(), Some(Code::CREATED));
    assert_eq!(
        capture.fields(),
        [
            (b"content-type".to_vec(), b"text/plain".to_vec()),
            (b"x-static".to_vec(), b"yes".to_vec()),
            (b"x-dynamic".to_vec(), b"value".to_vec()),
        ]
    );
    assert_eq!(capture.body(), b"body");
}

#[test]
fn templates_stay_semantic() {
    let result = Shared::try_copy(b"semantic");
    let body = result.expect("the response body must allocate");
    let response = fixed::Direct::new(Code::OK, Head::new(&SEMANTIC, Set::<0>::new()), body);

    let capture = response::Capture::try_from(response).expect("response encoding");
    assert_eq!(
        capture.fields(),
        [
            (b"x-first".to_vec(), b"one".to_vec()),
            (b"x-second".to_vec(), b" two".to_vec()),
        ]
    );
    assert_eq!(capture.body(), b"semantic");
}

#[test]
fn retained_keeps_pointer() {
    let result = Shared::try_copy(b"retained");
    let shared = result.expect("the retained body must allocate");
    let pointer = shared.as_ref().as_ptr() as usize;
    let response = fixed::Direct::new(Code::OK, (), shared);

    let capture = response::Capture::try_from(response).expect("response encoding");
    assert_eq!(capture.pointer(), Some(pointer));
}

#[test]
fn segments_remain_distinct() {
    let result = (Shared::try_copy(b"first"), Shared::try_copy(b"second"));
    assert!(
        matches!(result, (Ok(_), Ok(_))),
        "both retained segments must allocate"
    );
    let (Ok(first), Ok(second)) = result else {
        return;
    };
    let pointers = [first.as_ref().as_ptr(), second.as_ref().as_ptr()];
    let body = owned::Segments::from(first);
    let result = body.try_extend(owned::Segments::from(second));
    let body = result.expect("the second segment must append");
    let response = fixed::Direct::new(Code::OK, (), body);

    assert_eq!(response.body().len(), 11);
    assert!(response.body().count() == segments::Count::TWO);
    assert_eq!(response.body().as_slice(), None);
    let segments = response.body().segments().collect::<Vec<_>>();
    assert_eq!(segments, [b"first".as_slice(), b"second".as_slice()]);
    assert_eq!(segments[0].as_ptr(), pointers[0]);
    assert_eq!(segments[1].as_ptr(), pointers[1]);

    let capture = response::Capture::try_from(response).expect("response encoding");
    assert_eq!(capture.body(), b"firstsecond");
    assert_eq!(capture.pointer(), None);
}

#[test]
fn result_keeps_bound() {
    type Reply<'a> = Result<statics::Direct<'a, ()>, fixed::Direct<(), Body<'a>>>;
    assert!(matches!(
        <Reply<'_> as Immediate<'_, '_>>::Bound::LIMIT,
        Limit::Unbounded
    ));
    let bytes = *b"on";
    let replies: [Reply<'_>; 2] = [
        Ok(statics::Direct::new(Code::OK, (), Body::new(&bytes[..1]))),
        Err(fixed::Direct::new(
            Code::BAD_REQUEST,
            (),
            Body::new(&bytes[1..]),
        )),
    ];
    for (response, (status, body)) in replies
        .into_iter()
        .zip([(Code::OK, &bytes[..1]), (Code::BAD_REQUEST, &bytes[1..])])
    {
        let capture = response::Capture::try_from(response).expect("response encoding");
        assert_eq!(capture.status(), Some(status));
        assert_eq!(capture.body(), body);
        assert_eq!(capture.calls(), 1);
    }
}

#[test]
fn stream_keeps_headers() {
    let value = *b"on";
    let stream = &value[..];
    let fields = Set::from_optional([
        None,
        Some(Item::borrow(
            field::literal!(Name::try_bytes(b"x-stream")),
            Value::try_bytes(&value).expect("stream field"),
        )),
        None,
    ]);
    let output = dispatch::Stream::new(stream)
        .with_status(Code::CREATED)
        .headers(fields);
    let (capture, retained) =
        dispatch::Emit::emit(output, response::Capture::default()).expect("stream response");
    assert_eq!(capture.status(), Some(Code::CREATED));
    assert_eq!(capture.fields(), [(b"x-stream".to_vec(), value.to_vec())]);
    assert_eq!(capture.calls(), 1);
    assert!(std::ptr::eq(retained, stream));
}

#[test]
fn encodes_once() {
    let encodes = Cell::new(0usize);
    let body = Counted {
        bytes: b"body",
        bound: 32,
        encodes: &encodes,
    };
    let output = bounded::Output::new(Code::OK, &EMPTY, Set::<0>::new(), body);
    let capture = response::Capture::try_from(output).expect("response encoding");
    assert_eq!(encodes.get(), 1);
    assert_eq!(capture.status(), Some(Code::OK));
    assert_eq!(capture.body(), b"body");
}

#[test]
fn rejects_bad_bound() {
    let encodes = Cell::new(0usize);
    let body = Counted {
        bytes: b"too large",
        bound: 1,
        encodes: &encodes,
    };
    let output = bounded::Output::new(Code::OK, &EMPTY, Set::<0>::new(), body);
    let result = response::Capture::try_from(output);
    assert!(matches!(
        result,
        Err((response::Error::Bounded(storage::BuildError::Build(_)), _))
    ));
    let Err((response::Error::Bounded(storage::BuildError::Build(error)), capture)) = result else {
        return;
    };
    assert_eq!(
        error.into_parts(),
        (NonZero::<usize>::MIN.saturating_add(8), 1)
    );
    assert_eq!(capture.calls(), 1);
    assert_eq!(encodes.get(), 1);
}

#[test]
fn output_carries_bound() {
    let bounds = Cell::new(0);
    let encodes = Cell::new(0);
    let plan = bounded::Plan::try_new(Checked {
        bytes: b"body",
        bound: Ok(32),
        bounds: &bounds,
        encodes: &encodes,
    })
    .expect("checked bound");
    let output: bounded::Output<'static, _, 0> =
        bounded::Output::new(Code::OK, &EMPTY, Set::new(), plan);
    let capture = response::Capture::try_from::<bounded::Output<'static, _, 0>>(output)
        .expect("response encoding");
    assert_eq!((bounds.get(), encodes.get()), (1, 1));
    assert_eq!(capture.body(), b"body");
}

#[test]
fn rejection_keeps_owner() {
    let bounds = Cell::new(0);
    let encodes = Cell::new(0);
    let error = exact::Overflow {
        current: usize::MAX,
        added: NonZero::<usize>::MIN,
    };
    let body = Checked {
        bytes: b"body",
        bound: Err(error),
        bounds: &bounds,
        encodes: &encodes,
    };
    let pointer = body.bytes.as_ptr();
    let Err(rejected) = bounded::Plan::try_new(body) else {
        panic!("bound rejected");
    };
    let (body, actual) = rejected.into_parts();
    assert_eq!(actual, error);
    assert_eq!(body.bytes.as_ptr(), pointer);
    assert_eq!((bounds.get(), encodes.get()), (1, 0));
}

#[test]
fn admits_status_extensions() {
    let first = Code::from_u16(status::FIRST).expect("first status");
    let extend = Code::from_u16(status::EXTENSION.0).expect("extension status");
    let last = Code::from_u16(status::LAST.0).expect("last status");
    assert_eq!(first, Code::CONTINUE);
    assert_eq!(extend.as_bytes(), status::EXTENSION.1);
    assert_eq!(last.as_bytes(), status::LAST.1);
    assert_eq!(last.reason(), None);
    assert_eq!(Code::OK.reason(), Some(status::OK_REASON));
    assert_eq!(
        Code::from_u16(status::SHORT)
            .expect_err("short status")
            .actual(),
        status::SHORT
    );
    assert_eq!(
        Code::from_u16(status::LONG)
            .expect_err("long status")
            .actual(),
        status::LONG
    );
}

#[test]
fn direct_underflow() {
    let mut bytes = [0xa5; 3];
    let error = exact::Plan::new(Short)
        .encode(&mut bytes)
        .expect_err("direct underflow must retain its exact span gap");
    let owned = exact::Plan::new(Short)
        .into_shared()
        .expect_err("owned underflow must retain its exact span gap");
    assert_eq!(error, owned);
    let error = match error {
        storage::BuildError::Length(error) => Some(error),
        _ => None,
    }
    .expect("underflow retains its length failure");
    let (expected, actual) = error.into_parts();
    assert_eq!((expected.get(), actual), (2, 1));
    assert_eq!(bytes, [b'x', 0xa5, 0xa5]);
}
