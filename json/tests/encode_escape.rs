use o3::buffer::Owned;
use sark_json::{Encode, Writer};

fn check(value: &[u8]) {
    let mut out = Owned::new();
    let mut w = Writer::new(&mut out, 0);
    w.put_str(value);
    w.finish();
    assert_eq!(
        Encode::str_len(value),
        out.as_ref().len(),
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
