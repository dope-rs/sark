use sark_core::http::EncodedBody;
use sark_json::{Encode, JsonBody, JsonEncode, Write, Writer};

struct LengthMismatch;

impl JsonEncode for LengthMismatch {
    fn json_len(&self) -> usize {
        0
    }

    fn write_into<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        writer.put(b"x")
    }
}

struct ShortWrite;

impl JsonEncode for ShortWrite {
    fn json_len(&self) -> usize {
        2
    }

    fn write_into<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        writer.put(b"x")
    }
}

fn check(value: &[u8]) {
    let mut out = Vec::new();
    let mut w = Writer::new(&mut out, 0);
    w.put_str(value).unwrap();
    w.finish();
    assert_eq!(
        Encode::str_len(value),
        out.len(),
        "str_len must match put_str output for {value:?}"
    );
}

#[test]
fn backspace_and_formfeed_are_two_bytes() {
    check(&[0x08]);
    check(&[0x0c]);
    check(b"a\x08b\x0cc");
}

#[test]
fn other_control_chars_are_six_bytes() {
    check(&[0x00]);
    check(&[0x1f]);
    check(&[0x07]);
}

#[test]
fn common_escapes_and_plain() {
    check(b"\"\\\n\r\t");
    check(b"hello world");
}

#[test]
fn long_write_returns_capacity_error() {
    assert!(JsonBody::new(LengthMismatch).encode_into(&mut []).is_err());
}

#[test]
fn short_write_returns_length_error() {
    assert!(JsonBody::new(ShortWrite).into_shared(2).is_err());
}
