use sark_http::field::text::{Ascii, IntError};

#[test]
fn usize_parser_bounds() {
    let max = usize::MAX.to_string();
    let overflow = format!("{max}0");

    assert!(Ascii::new(b"000000000000000000000").parse_usize() == Ok(0));
    assert!(Ascii::new(max.as_bytes()).parse_usize() == Ok(usize::MAX));
    assert!(Ascii::new(overflow.as_bytes()).parse_usize() == Err(IntError::Overflow));
    assert!(Ascii::new(b"").parse_usize() == Err(IntError::Empty));
    assert!(Ascii::new(b"+1").parse_usize() == Err(IntError::InvalidByte));
    assert!(Ascii::new(b"42x").parse_prefix() == Ok((42, 2)));
    assert!(Ascii::new(b"x42").parse_prefix() == Err(IntError::InvalidByte));
}

#[test]
fn u64_parser_bounds() {
    let max = u64::MAX.to_string();
    let overflow = format!("{max}0x");

    assert!(Ascii::new(b"9999999999999999999").parse_u64() == Ok(9_999_999_999_999_999_999));
    assert!(Ascii::new(max.as_bytes()).parse_u64() == Ok(u64::MAX));
    assert!(Ascii::new(overflow.as_bytes()).parse_u64() == Err(IntError::Overflow));
    assert!(Ascii::new(b"42\n").parse_u64() == Err(IntError::InvalidByte));
}
