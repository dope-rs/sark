use std::mem::MaybeUninit;

use sark_http::h1::codec::numbers::Wire as _;

#[test]
fn initializes_decimal_suffix() {
    for value in [0usize, 9, 10, 99, 100, usize::MAX] {
        let expected = value.to_string();
        let mut out = [MaybeUninit::uninit(); 20];
        let digits = value.decimal_uninit(&mut out);
        assert_eq!(digits, expected.as_bytes());
        assert_eq!(digits.len(), value.decimal_len().get());
        let mut padded = [b'0'; 20];
        assert_eq!(value.decimal(&mut padded), expected.as_bytes());
        assert!(
            padded[..20 - expected.len()]
                .iter()
                .all(|byte| *byte == b'0')
        );
    }
}
