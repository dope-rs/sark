pub mod common;

use std::ops;

use common::wire::base64::{VECTORS, encode, try_decode};
use o3::buffer::write::{Sink as _, Staged};
use sark_http::field::{
    Encode as _,
    base64::{Decoder, Encoded, Encoder, Error},
};

#[test]
fn accepts_padding_forms() {
    for &(raw, plain, padded) in VECTORS {
        assert_eq!(encode(raw, false), plain);
        assert_eq!(encode(raw, true), padded);
        for (value, expected) in [(Encoder::new(raw), plain), (Encoder::padded(raw), padded)] {
            let len = value.encoded_len();
            let mut out = Vec::with_capacity(len);
            let mut writer = Staged::try_new(&mut out, len).expect("admitted capacity");
            value
                .try_write(|bytes| writer.write_slice(bytes))
                .expect("field emission");
            writer.try_complete().expect("exact field length").commit();
            assert_eq!(out, expected);
        }
        assert!(try_decode(plain).as_deref() == Ok(raw));
        assert!(try_decode(padded).as_deref() == Ok(raw));
    }
}

#[test]
fn every_octet_round_trips() {
    let raw = (0..=u8::MAX).collect::<Vec<_>>();
    for padded in [false, true] {
        let encoded = encode(&raw, padded);
        assert!(Encoded::new(&encoded).decoded_len() == Ok(raw.len()));
        assert!(try_decode(&encoded) == Ok(raw.clone()));
    }
}

#[test]
fn decodes_overlaps_safely() {
    let encoded = b"AQIDBAUG";
    for dst in [0, 8, 10] {
        let mut owner = [0xa5; 24];
        owner[8..16].copy_from_slice(encoded);
        assert!(Decoder::new(&mut owner).decode(8..16, dst) == Ok(6));
        assert_eq!(&owner[dst..dst + 6], &[1, 2, 3, 4, 5, 6]);
    }

    let raw = (0..96).collect::<Vec<_>>();
    let encoded = encode(&raw, false);
    let mut owner = vec![0xa5; encoded.len() + 32];
    owner[32..].copy_from_slice(&encoded);
    let end = owner.len();
    assert!(Decoder::new(&mut owner).decode(32..end, 0) == Ok(raw.len()));
    assert_eq!(&owner[..raw.len()], raw);
}

#[test]
fn matches_all_overlaps() {
    for raw_len in 0..=96 {
        let raw = (0..raw_len)
            .map(|index| (index * 37 % 256) as u8)
            .collect::<Vec<_>>();
        for padded in [false, true] {
            let encoded = encode(&raw, padded);
            let source = 17;
            let dests = [
                0,
                source - 1,
                source,
                source + 1,
                source + encoded.len() / 2,
                source + encoded.len() + 1,
            ];
            for dst in dests {
                let src_end = source + encoded.len();
                let dst_end = dst + raw.len();
                let mut owner = vec![0xa5; src_end.max(dst_end) + 17];
                owner[source..src_end].copy_from_slice(&encoded);
                assert!(Decoder::new(&mut owner).decode(source..src_end, dst) == Ok(raw.len()));
                assert_eq!(&owner[dst..dst + raw.len()], raw);
            }
        }
    }
}

#[test]
fn validates_octets_and_offsets() {
    for byte in 0..=u8::MAX {
        for index in 0..64 {
            let mut encoded = [b'A'; 68];
            encoded[index] = byte;
            let expected = if byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/') {
                Ok(51)
            } else {
                Err(Error::Byte { index, byte })
            };
            assert!(Encoded::new(&encoded).validate() == expected);
        }
    }
}

#[test]
fn rejects_malformed_text() {
    assert!(try_decode(b"A") == Err(Error::Length { actual: 1 }));
    assert!(
        try_decode(b"A===")
            == Err(Error::Padding {
                index: 1,
                byte: b'=',
            })
    );
    assert!(
        try_decode(b"AA=A")
            == Err(Error::Byte {
                index: 2,
                byte: b'=',
            })
    );
    assert!(
        try_decode(b"AB")
            == Err(Error::Padding {
                index: 1,
                byte: b'B',
            })
    );
    assert!(
        try_decode(b"AA?A")
            == Err(Error::Byte {
                index: 2,
                byte: b'?',
            })
    );
    assert!(
        Encoded::new(b"dGhlIHNhbXBsZSBub25jZR==").validate()
            == Err(Error::Padding {
                index: 21,
                byte: b'R',
            })
    );
    assert!(
        Encoded::new(b"dGhlIHNhbXBsZSBub25jZQ=A").validate()
            == Err(Error::Byte {
                index: 22,
                byte: b'=',
            })
    );

    let mut lane = *b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    lane[17] = b'?';
    assert!(
        try_decode(&lane)
            == Err(Error::Byte {
                index: 17,
                byte: b'?'
            })
    );
}

#[test]
fn reports_storage_bounds() {
    let mut owner = *b"AAAA";
    assert!(
        Decoder::new(&mut owner).decode(ops::Range { start: 3, end: 2 }, 0)
            == Err(Error::Source {
                start: 3,
                end: 2,
                capacity: 4,
            })
    );
    assert!(
        Decoder::new(&mut owner).decode(0..4, 2)
            == Err(Error::Output {
                start: 2,
                len: 3,
                capacity: 4,
            })
    );
    assert!(
        Decoder::new(&mut owner).decode(0..4, usize::MAX)
            == Err(Error::Output {
                start: usize::MAX,
                len: 3,
                capacity: 4,
            })
    );
}
