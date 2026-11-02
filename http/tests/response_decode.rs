pub mod common;

use common::response::{coding, headers};
use dope_test::alloc::Tracker;
use sark_http::{
    field::Value,
    h1::{
        codec::decode::response::Progress,
        head::{Limit, response::State},
    },
    payload::response::{Method, Response, status::Code},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn body_allocation_failures() {
    use sark_http::h1::codec::decode::response::{Decoder, Error};

    let decoder = Decoder::new(Method::Other);
    assert!(matches!(
        Tracker::fail_after(0, || decoder.finish(headers::EOF_BODY)),
        Err(Error::Allocation(o3::alloc::Exhausted))
    ));
}

#[test]
fn header_allocation_failures() {
    use sark_http::{
        h1::codec::decode::fields::{self, Sink as _},
        payload::response::headers::List,
    };

    let mut list = List::new();
    assert!(matches!(
        Tracker::fail_after(1, || list.push(b"content-type", headers::OPAQUE)),
        Err(fields::Error::Allocation(o3::alloc::Error::Exhausted))
    ));
    assert!(list.is_empty());
}

#[test]
fn constructors_keep_parts() {
    let resp = Response::new(Code::OK);

    assert_eq!(resp.status(), Code::OK);

    let resp = Response::new(Code::CREATED).with_body(b"Created resource".to_vec());

    assert_eq!(resp.status(), Code::CREATED);
    assert_eq!(resp.body().as_slice(), b"Created resource");
}

#[test]
fn trailers_stay_separate() {
    use sark_http::h1::codec::decode::response::Decoder;

    let wire = headers::TRAILERS;
    {
        let response =
            match (Decoder::new(Method::Other).decode(wire)).expect("response must decode") {
                Progress::Complete(value) => Some(value),
                Progress::More => None,
            }
            .expect("test input must be complete");
        assert_eq!(
            response.headers().get("x-head").map(Value::as_bytes),
            Some(b"head".as_slice()),
        );
        assert!(!response.headers().contains_key("x-tail"));
        assert_eq!(
            response.trailers().get("x-tail").map(Value::as_bytes),
            Some(headers::OPAQUE),
        );
    };
}

#[test]
fn header_repeats_preserved() {
    use sark_http::h1::codec::decode::response::Decoder;

    let wire = headers::REPEATED;
    {
        let response =
            match (Decoder::new(Method::Other).decode(wire)).expect("response must decode") {
                Progress::Complete(value) => Some(value),
                Progress::More => None,
            }
            .expect("test input must be complete");
        let cookies: Vec<_> = response
            .headers()
            .iter()
            .filter(|entry| entry.0.as_str() == "set-cookie")
            .map(|entry| entry.1.as_bytes())
            .collect();

        assert_eq!(cookies, [b"a=1".as_slice(), headers::OPAQUE]);
        assert_eq!(
            response.headers().get("Set-Cookie").map(Value::as_bytes),
            Some(b"a=1".as_slice())
        );
        assert_eq!(response.headers().len(), 3);
        let names: Vec<_> = response
            .headers()
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(names, ["set-cookie", "set-cookie", "content-length"]);
    };
}

#[test]
fn status_code() {
    use sark_http::h1::codec::decode::response::Decoder;

    let wire = headers::STATUS;
    {
        let response =
            match (Decoder::new(Method::Other).decode(wire)).expect("response must decode") {
                Progress::Complete(value) => Some(value),
                Progress::More => None,
            }
            .expect("test input must be complete");
        assert_eq!(response.status().as_u16(), 599);
    };
}

#[test]
fn decoded_headers_own_data() {
    use sark_http::h1::codec::decode::response::Decoder;

    let response = {
        let wire = headers::TRAILERS.to_vec();
        Decoder::new(Method::Other)
            .finish(&wire)
            .expect("complete owned response")
    };
    assert_eq!(
        response.headers().get("x-head").map(Value::as_bytes),
        Some(b"head".as_slice())
    );
    assert_eq!(
        response.trailers().get("x-tail").map(Value::as_bytes),
        Some(headers::OPAQUE)
    );
}

#[test]
fn coding_fragments() {
    for (wire, expected) in coding::CASES {
        State::with(Vec::new(), Limit::RESPONSE, |mut source, mut progress| {
            for end in 0..wire.len() {
                let (next, head) = progress.scan(&source).expect("partial head");
                progress = next;
                assert!(head.is_none());
                source.try_extend(&wire[end..=end]).expect("append byte");
            }
            let (_, head) = progress.scan(&source).expect("response head");
            let admitted = head
                .expect("complete head")
                .admit(Method::Other)
                .expect("response framing");
            assert_eq!(admitted.coding(), expected);
        });
    }
}
