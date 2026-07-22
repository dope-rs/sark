use sark_core::http::codec::Header;

#[test]
fn content_length_accepts_digits() {
    assert_eq!(Header::content_length(b"0").unwrap(), 0);
    assert_eq!(Header::content_length(b"1234").unwrap(), 1234);
}

#[test]
fn content_length_rejects_invalid_values() {
    for value in [
        b"".as_slice(),
        b"+5",
        b" 5",
        b"5 ",
        b"5\r",
        b"abc",
        b"5a",
        b"99999999999999999999999999",
    ] {
        assert!(Header::content_length(value).is_err());
    }
}
