use common::hpack::Encoded;
pub mod common;

use sark_h2::hpack::decode::{Decoder, Error};
use sark_http::field::metadata::integer::PrefixedInt;

#[test]
fn rejects_huge_lengths() {
    let cases: [u64; 8] = [
        u64::MAX,
        u64::MAX - 1,
        u64::MAX - 6,
        u64::MAX - 11,
        0xFFFF_FFFF_FFFF_FFFA,
        0x8000_0000_0000_0000,
        0xFFFF_FFFF_0000_0000,
        0x0000_0001_0000_0000,
    ];
    for &len in &cases {
        let mut bytes = vec![0x00u8];
        PrefixedInt::<7>::new(len).encode(0x00, &mut bytes);
        bytes.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
        let mut fields = 0;
        let res = dec.decode_limit(&bytes, |_, _| fields += 1);
        assert!(
            res.is_err(),
            "len={len:#x} must be a clean error, not a panic"
        );
        assert_eq!(fields, 0);
    }
}

#[test]
fn rejects_large_length() {
    let mut bytes = vec![0x00u8];
    PrefixedInt::<7>::new((1u64 << 24) + 1).encode(0x00, &mut bytes);
    bytes.extend_from_slice(&[0u8; 16]);
    let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let mut fields = 0;
    let result = dec.decode_limit(&bytes, |_, _| fields += 1);
    assert!(matches!(result, Err(Error::BadString)));
    assert_eq!(fields, 0);
}

#[test]
fn decodes_normal_literal() {
    let bytes: &[u8] = &[0x00, 0x03, b'f', b'o', b'o', 0x03, b'b', b'a', b'r'];
    let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let (_, got) = Encoded::new(bytes)
        .try_collect(dec)
        .expect("normal literal decodes");
    assert_eq!(got, vec![(b"foo".to_vec(), b"bar".to_vec())]);
}
