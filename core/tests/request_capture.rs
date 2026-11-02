pub mod common;

use common::requests;
use sark_core::{
    request::{field, frame::raw::Path, path},
    route::raw::PathCapture,
};
use sark_http::{field::text::IntError, request::span::Span};

#[test]
fn capture_offsets() {
    let request = requests::Schema::try_parse(b"/items/42/42x").expect("request capture fixture");
    // SAFETY: these ranges are the two exact numeric segments in this request.
    let valid = unsafe { Path::new(&request, PathCapture::new(7, 9)) };
    assert_eq!(valid.try_parse::<usize>(), Ok(42));
    let invalid = unsafe { Path::new(&request, PathCapture::new(10, 13)) };
    assert_eq!(
        invalid.try_parse::<usize>(),
        Err(field::Error::Integer {
            start: 10,
            end: 13,
            cause: IntError::InvalidByte,
        }),
    );
}

#[test]
fn capture_bounds() {
    let target = path::Target::new(b"/42");
    assert_eq!(
        <usize as field::Path>::try_parse(&target, 1..4),
        Err(field::Error::Path(path::Error::Bounds {
            start: 1,
            end: 4,
            capacity: 3,
        })),
    );
}

#[test]
fn header_bounds() {
    let span = Span::checked(9_000, 100).expect("representable out-of-bounds header span");
    let cause = field::SliceValue::try_at(requests::HEAD, span)
        .err()
        .expect("invalid header extent must be rejected before schema projection");
    assert_eq!(
        cause,
        field::Error::Bounds {
            start: 9_000,
            end: 9_100,
            capacity: requests::HEAD.len(),
        }
    );
}
