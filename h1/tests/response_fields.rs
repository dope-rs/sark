pub mod common;

use std::cell::Cell;

use common::fields::{
    Counted, DATE, ENCODED_BODY, ENTRIES, GENERATED_IDENTITY, GROUP, GROUP_ENCODING, IDENTITY,
    RESPONSE, try_encode,
};
use o3::buffer::write::Slice;
use sark_h1::wire::{Failure, output};
use sark_http::field;
use sark_http::payload::{
    fixed::Body,
    response::{Static, headers, status::Code},
};

#[test]
fn head_emits_once() {
    let emits = Cell::new(0);
    let response = Static::new(Code::OK, Counted { emissions: &emits }, Body::new(b"body"));
    let mut bytes = [0; 512];
    let mut writer = Slice::new(&mut bytes);
    let outcome = output::Egress::new(writer.reborrow(), DATE, RESPONSE).try_route(
        response,
        &mut (),
        common::fields::PLAIN,
    );
    let Ok(output::Complete::Static { .. }) = outcome else {
        panic!("valid static response must be emitted");
    };

    assert_eq!(emits.get(), 1);
    assert!(
        writer
            .into_mut()
            .windows(b"x-actual: present\r\n".len())
            .any(|window| window == b"x-actual: present\r\n")
    );
}

#[test]
fn grouped_capacity() {
    let mut expected = [0xa5; 256];
    let mut writer = Slice::new(&mut expected);
    let response = Static::new(Code::OK, ENTRIES, Body::new(b"body"));
    let length = match output::Egress::new(writer.reborrow(), DATE, RESPONSE).try_route(
        response,
        &mut (),
        common::fields::PLAIN,
    ) {
        Ok(output::Complete::Static { .. }) => Some(writer.len()),
        _ => None,
    }
    .expect("the complete scalar header must fit");

    for capacity in 0..=length {
        let mut scalar = [0xa5; 256];
        let mut grouped = scalar;
        let mut scalar_writer = Slice::new(&mut scalar[..capacity]);
        let mut grouped_writer = Slice::new(&mut grouped[..capacity]);
        let response = Static::new(Code::OK, ENTRIES, Body::new(b"body"));
        let first = output::Egress::new(scalar_writer.reborrow(), DATE, RESPONSE).try_route(
            response,
            &mut (),
            common::fields::PLAIN,
        );
        let response = Static::new(Code::OK, GROUP, Body::new(b"body"));
        let second = output::Egress::new(grouped_writer.reborrow(), DATE, RESPONSE).try_route(
            response,
            &mut (),
            common::fields::PLAIN,
        );
        let results = match (first, second) {
            (Err(Failure::Output(scalar)), Err(Failure::Output(grouped))) => {
                Some((Err(scalar), Err(grouped)))
            }
            (Ok(output::Complete::Static { .. }), Ok(output::Complete::Static { .. })) => {
                Some((Ok(scalar_writer.len()), Ok(grouped_writer.len())))
            }
            _ => None,
        };
        let (left, right) = results.expect("matching output variants");
        assert_eq!(right, left, "capacity {capacity}");
        assert_eq!(right.is_ok(), capacity == length);
        assert_eq!(grouped, scalar, "capacity {capacity}");
    }
}

#[test]
fn encoding_preserves_body() {
    preserve([IDENTITY]);
    preserve(GROUP_ENCODING);
    preserve(GENERATED_IDENTITY);
}

fn preserve(source: impl field::Source<headers::Application>) {
    let input = ENCODED_BODY;
    let outcome = try_encode(source, &input);
    let Ok(output::Complete::Static { body }) = outcome else {
        panic!("an existing encoding must preserve the original body");
    };
    assert_eq!(body.as_slice(), input);
    assert_eq!(body.as_slice().as_ptr(), input.as_ptr());
}
