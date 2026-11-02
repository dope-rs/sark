use sark_http::field::text::{Ascii, IntError};

#[test]
fn usize_parser_bounds() {
    let max = usize::MAX.to_string();
    let overflow = format!("{max}0");

    assert!(Ascii::new(b"0").parse_usize() == Ok(0));
    assert!(Ascii::new(max.as_bytes()).parse_usize() == Ok(usize::MAX));
    assert!(Ascii::new(overflow.as_bytes()).parse_usize() == Err(IntError::Overflow));
    assert!(Ascii::new(b"").parse_usize() == Err(IntError::Empty));
    assert!(Ascii::new(b"+1").parse_usize() == Err(IntError::InvalidByte));
}

#[test]
fn u64_parser_bounds() {
    let max = u64::MAX.to_string();
    let overflow = format!("{max}0");

    assert!(Ascii::new(b"42").parse_u64() == Ok(42));
    assert!(Ascii::new(max.as_bytes()).parse_u64() == Ok(u64::MAX));
    assert!(Ascii::new(overflow.as_bytes()).parse_u64() == Err(IntError::Overflow));
    assert!(Ascii::new(b"42\n").parse_u64() == Err(IntError::InvalidByte));
}
