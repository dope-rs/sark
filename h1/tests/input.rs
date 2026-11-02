pub mod common;

use common::fields;
use o3::buffer::{Complete as _, storage};
use sark_core::route::guard::Reject as _;
use sark_h1::{
    Input,
    wire::{output, parsing::Head},
};
use sark_http::payload::response::{fixed, headers::Entry, status::Code};

#[test]
fn retains_guard_window() {
    let mut bytes = [b'a'; 1024];
    bytes[192..704].fill(b'b');
    let owner = storage::Shared::try_copy(&bytes);
    let owner = owner.expect("test operation must succeed");
    let window = owner.view().try_get(128..896);
    let window = window.expect("test operation must succeed");
    let source = &window.as_slice()[64..576];
    let fields: [Entry<'_>; 0] = [];
    let response = fixed::Direct::new(Code::OK, fields, source);
    let mut output = [0; 128];
    let result = output::Egress::bind(
        &mut output,
        b"Mon, 01 Jan 2026 00:00:00 GMT",
        Input::Buffered(window),
        fields::RESPONSE,
    )
    .try_reject(response);
    let output::Outcome::SendShared { body, .. } = result else {
        panic!("guard response must retain its buffered owner");
    };
    assert_eq!(body.as_slice().as_ptr(), source.as_ptr());
    drop(owner);
    assert_eq!(body.as_slice(), &[b'b'; 512]);
}

#[test]
fn copies_guard_window() {
    let mut owner = [b'a'; 1024];
    owner[192..704].fill(b'b');
    let window = &owner[128..896];
    let source = &window[64..576];
    let fields: [Entry<'_>; 0] = [];
    let response = fixed::Direct::new(Code::OK, fields, source);
    let mut output = [0; 128];
    let result = output::Egress::bind(
        &mut output,
        b"Mon, 01 Jan 2026 00:00:00 GMT",
        Input::Borrowed(window),
        fields::RESPONSE,
    )
    .try_reject(response);
    let output::Outcome::SendOwnedCopy { body, .. } = result else {
        panic!("guard response must copy its borrowed body");
    };
    assert_ne!(body.as_slice().as_ptr(), source.as_ptr());
    owner.fill(0);
    assert_eq!(body.as_slice(), &[b'b'; 512]);
}

#[test]
fn parses_original_window() {
    let wire = b"GET /route?name=value HTTP/1.1\r\n\r\n";
    let mut bytes = b"ignored!".to_vec();
    bytes.extend_from_slice(wire);
    bytes.extend_from_slice(b"trailing");
    let owner = storage::Shared::try_copy(&bytes).expect("source allocation");
    let window = owner
        .view()
        .try_get(8..8 + wire.len())
        .expect("request window");
    for input in [Input::Borrowed(wire), Input::Buffered(window)] {
        let (source, path, query) = {
            let parsed = Head::parse(input, u8::MAX)
                .ok()
                .flatten()
                .expect("complete request");
            (
                parsed.input().as_slice(),
                parsed.request().path().expect("origin path"),
                parsed.request().plan().query_span().expect("query span"),
            )
        };
        assert_eq!(source, wire);
        assert_eq!(source.as_ptr(), input.as_slice().as_ptr());
        assert_eq!(path, b"/route");
        assert_eq!(path.as_ptr(), source[4..].as_ptr());
        assert_eq!(&source[query.start()..query.end()], b"name=value");
    }
}
