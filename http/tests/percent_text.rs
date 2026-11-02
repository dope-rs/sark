pub mod common;

use common::field::percent::{REPAIRED, WINDOWS};
use o3::buffer::write::{Sink as _, Staged};
use percent::decoder::Decoder;
use sark_http::field::{Encode as _, text::percent};

#[test]
fn rejects_reversed_source() {
    let mut bytes = [0; 8];
    assert!(matches!(
        Decoder::new(&mut bytes, std::ops::Range { start: 6, end: 4 }, 0),
        Err(percent::decoder::Error::Bounds {
            capacity: 8,
            source: std::ops::Range { start: 6, end: 4 },
            destination: 0,
        })
    ));
}

#[test]
fn encodes_field_text() {
    use percent::raw;
    let text = "literal %\r\n\0\u{2603}";
    let encoder = raw::Encoder::new(text).expect("representable text");
    let len = encoder.encoded_len();
    let mut wire = Vec::with_capacity(len);
    let mut writer = Staged::try_new(&mut wire, len).expect("admitted capacity");
    encoder
        .try_write(|bytes| writer.write_slice(bytes))
        .expect("field emission");
    writer.try_complete().expect("exact field length").commit();
    assert_eq!(wire, b"literal %25%0D%0A%00%E2%98%83");
}

#[test]
fn repairs_invalid_utf8() {
    for &(wire, expected) in REPAIRED {
        for &(source, dst) in WINDOWS {
            let end = source + wire.len();
            let mut bytes = vec![b'!'; end + 3];
            bytes[source..end].copy_from_slice(wire);
            let written = Decoder::new(&mut bytes, source..end, dst)
                .expect("nonexpanding decode window")
                .decode()
                .range();
            assert_eq!(written, dst..dst + expected.len());
            assert_eq!(&bytes[written], expected.as_bytes());
            assert!(bytes[..dst].iter().all(|byte| *byte == b'!'));
            assert!(bytes[end..].iter().all(|byte| *byte == b'!'));
        }
    }
}
