use dope_test::alloc::Tracker;
use sark_h3::qpack::encoder::Encoder;
use sark_http::field::{literal, metadata::strings::Encoding};

use crate::common::field::entry;

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn huffman_wire_reuses() {
    let fields = [literal!(entry(b":authority", b"www.example.com"))];
    let mut encoder = Encoder::new();
    encoder.set_encoding(Encoding::Huffman);
    let mut wire = Vec::new();
    encoder
        .encode(0, fields, &mut wire)
        .expect("Huffman section");
    assert_eq!(
        wire,
        b"\x00\x00\x50\x8c\xf1\xe3\xc2\xe5\xf2\x3a\x6b\xa0\xab\x90\xf4\xff"
    );
    wire.clear();
    let (_, allocs) = Tracker::count(|| {
        encoder
            .encode(0, fields, &mut wire)
            .expect("reused section")
    });
    assert_eq!(allocs, 0);
}
