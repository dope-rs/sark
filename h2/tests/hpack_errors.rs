use common::hpack::{CollectError, Encoded};
pub mod common;

use sark_h2::hpack::decode::{Decoder, Error};

#[test]
fn bad_index_zero() {
    let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let bytes: &[u8] = &[0x80];
    assert!(matches!(
        Encoded::new(bytes).try_collect(dec),
        Err(CollectError::Decode(Error::BadIndex))
    ));
}

#[test]
fn rejects_large_update() {
    let dec = Decoder::try_new(256).expect("HPACK decoder capacity");
    let bytes: &[u8] = &[0x3f, 0xe1, 0x1f];
    assert!(matches!(
        Encoded::new(bytes).try_collect(dec),
        Err(CollectError::Decode(Error::Resize {
            actual: 4096,
            limit: 256
        }))
    ));
}

#[test]
fn rejects_truncated_string() {
    let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let bytes: &[u8] = &[0x40, 0x0a, b'c', b'u', b's', b't', b'o', b'm'];
    assert!(matches!(
        Encoded::new(bytes).try_collect(dec),
        Err(CollectError::Decode(Error::NeedMore))
    ));
}

#[test]
fn rejects_integer_overflow() {
    let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let mut bytes = vec![0xff; 13];
    bytes.push(0x7f);
    assert!(matches!(
        Encoded::new(&bytes).try_collect(dec),
        Err(CollectError::Decode(Error::BadInteger(_)))
    ));
}
