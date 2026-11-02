use common::hpack::Encoded;
pub mod common;

use sark_h2::hpack::decode::Decoder;

#[test]
fn literal_with_index() {
    let bytes: &[u8] = &[
        0x40, 0x0a, b'c', b'u', b's', b't', b'o', b'm', b'-', b'k', b'e', b'y', 0x0d, b'c', b'u',
        b's', b't', b'o', b'm', b'-', b'h', b'e', b'a', b'd', b'e', b'r',
    ];
    let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let (dec, got) = Encoded::new(bytes)
        .try_collect(dec)
        .expect("valid HPACK fixture");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0, b"custom-key");
    assert_eq!(got[0].1, b"custom-header");
    assert_eq!(dec.dyn_size(), 55);
    assert_eq!(dec.dyn_len(), 1);
    let (n, v) = dec.dyn_get(0).expect("valid HPACK fixture");
    assert!(n.is_equal(b"custom-key"));
    assert!(v.is_equal(b"custom-header"));
}

#[test]
fn literal_without_index() {
    let bytes: &[u8] = &[
        0x04, 0x0c, b'/', b's', b'a', b'm', b'p', b'l', b'e', b'/', b'p', b'a', b't', b'h',
    ];
    let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let (dec, got) = Encoded::new(bytes)
        .try_collect(dec)
        .expect("valid HPACK fixture");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0, b":path");
    assert_eq!(got[0].1, b"/sample/path");
    assert_eq!(dec.dyn_size(), 0);
    assert_eq!(dec.dyn_len(), 0);
}

#[test]
fn literal_never_indexed() {
    let bytes: &[u8] = &[
        0x10, 0x08, b'p', b'a', b's', b's', b'w', b'o', b'r', b'd', 0x06, b's', b'e', b'c', b'r',
        b'e', b't',
    ];
    let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let (dec, got) = Encoded::new(bytes)
        .try_collect(dec)
        .expect("valid HPACK fixture");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0, b"password");
    assert_eq!(got[0].1, b"secret");
    assert_eq!(dec.dyn_size(), 0);
}
