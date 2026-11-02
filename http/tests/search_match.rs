use std::ptr;

use sark_http::field::scan::Match;

#[test]
fn keeps_exact_source() {
    for (bytes, byte, index) in [
        (b"|tail".as_slice(), b'|', 0),
        (b"head|tail".as_slice(), b'|', 4),
        (b"head\0".as_slice(), 0, 4),
    ] {
        for found in [Match::find(byte, bytes), Match::find2(0xff, byte, bytes)] {
            let found = found.expect("the fixture contains its delimiter");
            assert_eq!(
                (found.start(), found.end().get(), found.byte()),
                (index, index + 1, byte)
            );
            assert!(ptr::eq(found.prefix(), &bytes[..index]));
            assert!(ptr::eq(found.through(), &bytes[..index + 1]));
            assert!(ptr::eq(found.tail(), &bytes[index..]));
            assert!(ptr::eq(found.suffix(), &bytes[index + 1..]));
        }
    }
    let found = Match::find2(b'|', b'?', b"a?b|c").expect("both delimiters are present");
    assert_eq!((found.byte(), found.start()), (b'?', 1));
    assert!(Match::find(0, &[]).is_none());
    assert!(Match::find2(b'|', b'?', b"neither").is_none());
}
