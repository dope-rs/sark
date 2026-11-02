use std::mem;

use sark_http::payload;

use crate::payload::fixed;

#[test]
fn is_one_slice() {
    assert_eq!(
        mem::size_of::<fixed::Body>(),
        mem::size_of::<&'static [u8]>()
    );
    assert_eq!(fixed::Body::new(b"static").as_slice(), b"static");
}
