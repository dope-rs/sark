pub mod common;

use std::num::NonZero;

use common::{
    field::huffman::{self, Reference as _},
    wire::integer,
};
use o3::buffer::{
    storage,
    write::{Slice, SpareExt as _},
};
use sark_http::field::metadata::{
    huffman::{
        decoder::{self, DecodeError, Decoder},
        source::{Encoded, Source},
    },
    integer::{Error, Overflow, PrefixedInt, Width},
};

#[test]
fn rfc_authority() {
    let raw = huffman::AUTHORITY;
    let expected = huffman::AUTHORITY_ENCODED;
    let source = Source::new(raw)
        .try_measure()
        .expect("representable encoding");
    let mut encoded = Vec::with_capacity(source.encoded_len());
    source.encode(&mut encoded);
    assert_eq!(source.encoded_len(), expected.len());
    assert_eq!(encoded, expected);
    assert!(matches!(Encoded::new(&encoded).decode(Vec::new()), Ok(decoded) if decoded == raw));
}

#[test]
fn every_symbol_round_trips() {
    let raw: Vec<u8> = (0..=u8::MAX).collect();
    let source = Source::new(&raw)
        .try_measure()
        .expect("representable encoding");
    let mut encoded = Vec::new();
    source.encode(&mut encoded);
    assert_eq!(source.encoded_len(), encoded.len());
    assert!(matches!(Encoded::new(&encoded).decode(Vec::new()), Ok(decoded) if decoded == raw));
    assert!(matches!(Encoded::new(&encoded).decoded_len(), Ok(len) if len == raw.len()));
}

#[test]
fn split_state_round_trips() {
    let raw: Vec<u8> = (0..=u8::MAX).collect();
    let source = Source::new(&raw);
    let mut encoded = Vec::new();
    source.encode(&mut encoded);

    for split in 0..=encoded.len() {
        let (decoder, decoded, head) = (Decoder::new()
            .feed_into(&encoded[..split], Vec::new())
            .map(|output| output.into_parts())
            .map_err(|failure| {
                (
                    failure.cause,
                    failure.progress.consumed(),
                    failure.progress.decoded(),
                )
            }))
        .expect("valid encoded prefix must decode");
        let (decoder, decoded, tail) = (decoder
            .feed_into(&encoded[split..], decoded)
            .map(|output| output.into_parts())
            .map_err(|failure| {
                (
                    failure.cause,
                    failure.progress.consumed(),
                    failure.progress.decoded(),
                )
            }))
        .expect("valid encoded suffix must decode");
        (decoder.finish())
            .as_ref()
            .expect("test operation must succeed");
        assert_eq!(head + tail, raw.len());
        assert_eq!(decoded, raw, "split at encoded byte {split}");

        let (counter, first) = (Decoder::new().feed_count(&encoded[..split]))
            .expect("valid encoded prefix must count");
        let (counter, second) =
            (counter.feed_count(&encoded[split..])).expect("valid encoded suffix must count");
        (counter.finish())
            .as_ref()
            .expect("test operation must succeed");
        assert_eq!(
            first + second,
            raw.len(),
            "count split at encoded byte {split}"
        );
    }
}

#[test]
fn rejects_eos_padding() {
    for encoded in huffman::BAD_TAILS {
        assert!(
            Encoded::new(encoded).decode(Vec::new())
                == Err(DecodeError::InvalidEncoding(decoder::Error))
        );
    }
}

#[test]
fn separates_sink_failure() {
    let mut encoded = Vec::new();
    Source::new(huffman::SHORT).encode(&mut encoded);
    let failure = Encoded::new(&encoded)
        .decode_with(Err::<(), _>)
        .expect_err("failing sink must reject the encoded input");
    assert!(failure.cause == DecodeError::Sink(b'a'));
    assert_eq!(failure.progress.consumed(), 0);
    assert_eq!(failure.progress.decoded(), 0);
}

#[test]
fn owner_paths_match() {
    let raw = huffman::RETAINED;
    let mut encoded = Vec::new();
    Source::new(raw).encode(&mut encoded);
    let encoded = Encoded::new(&encoded);

    assert!(matches!(encoded.decoded_len(), Ok(len) if len == raw.len()));

    let mut owner =
        (storage::Owned::try_with(raw.len() + 16)).expect("the destination must allocate");
    let mut writer = owner.spare();
    let bounded = writer.bounded();
    let (bounded, decoded) = encoded
        .write_to(bounded)
        .map_err(|failure| {
            (
                failure.cause,
                failure.progress.consumed(),
                failure.progress.decoded(),
            )
        })
        .expect("valid input must fit its output owner");
    assert_eq!(decoded, raw.len());
    bounded.commit();
    writer.commit();
    assert_eq!(owner.as_slice(), raw);
}

#[test]
fn short_owner_rolls_back() {
    let mut encoded = Vec::new();
    Source::new(huffman::LONG).encode(&mut encoded);
    let mut owner =
        (storage::Owned::try_with(3)).expect("the three-byte destination must allocate");
    let mut writer = owner.spare();
    {
        let failure = (Encoded::new(&encoded).write_to(writer.bounded()))
            .err()
            .expect("the bounded sink must reject longer output");
        let error = match failure.cause {
            DecodeError::Sink(error) => Some(error),
            _ => None,
        }
        .expect("valid encoding must fail at the bounded sink");
        assert_eq!(error.into_parts(), (NonZero::<usize>::MIN, 0));
        assert_eq!(failure.progress.decoded(), 3);
        assert!(failure.progress.consumed() < encoded.len());
    }
    assert_eq!(writer.len(), 0);
    assert!(owner.as_slice().is_empty());
}

#[test]
fn invalid_owner_rolls_back() {
    let invalid = huffman::INVALID;
    let mut owner =
        (storage::Owned::try_with(16)).expect("the sixteen-byte destination must allocate");
    let mut writer = owner.spare();
    {
        let failure = (Encoded::new(invalid).write_to(writer.bounded()))
            .err()
            .expect("invalid input must be rejected");
        assert!(matches!(
            failure.cause,
            DecodeError::InvalidEncoding(decoder::Error)
        ));
        assert_eq!(failure.progress.decoded(), 3);
        assert!(failure.progress.consumed() < invalid.len());
    }
    assert_eq!(writer.len(), 0);
    assert!(owner.as_slice().is_empty());
}

#[test]
fn keeps_prefix() {
    let mut out = Vec::with_capacity(huffman::PREFIX.len() + huffman::AUTHORITY.len());
    out.extend_from_slice(huffman::PREFIX);
    let pointer = out.as_ptr();
    let out = Encoded::new(huffman::AUTHORITY_ENCODED)
        .decode(out)
        .expect("valid encoded suffix");
    assert_eq!(out.as_ptr(), pointer);
    assert_eq!(&out[..huffman::PREFIX.len()], huffman::PREFIX);
    assert_eq!(&out[huffman::PREFIX.len()..], huffman::AUTHORITY);
}

#[test]
fn dfa_matches_reference() {
    for byte in 0..=u8::MAX {
        let input = [byte];
        assert!(
            Encoded::new(&input).decode(Vec::new())
                == input.try_decode().map_err(DecodeError::InvalidEncoding),
            "{input:02x?}"
        );
    }

    let mut random = 0x9e37_79b9_7f4a_7c15u64;
    let mut input = [0u8; 8];
    for case in 0..8192usize {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let len = case % input.len() + 1;
        for byte in &mut input[..len] {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            *byte = random as u8;
        }
        assert!(
            Encoded::new(&input[..len]).decode(Vec::new())
                == input[..len]
                    .try_decode()
                    .map_err(DecodeError::InvalidEncoding),
            "{:02x?}",
            &input[..len]
        );
    }
}

#[test]
fn roundtrips_boundaries() {
    check::<1>(0x80);
    check::<2>(0x80);
    check::<3>(0x80);
    check::<4>(0x80);
    check::<5>(0x80);
    check::<6>(0x80);
    check::<7>(0x80);
    check::<8>(0x00);
}

#[test]
fn separates_decode_errors() {
    for bytes in [integer::TRUNCATED, integer::OVERFLOW] {
        let mut input = bytes;
        assert!(
            PrefixedInt::<5>::take(&mut input)
                == PrefixedInt::<5>::decode(bytes).map(|(value, _)| value)
        );
        assert_eq!(input, bytes);
    }
    assert!(PrefixedInt::<5>::decode(integer::TRUNCATED) == Err(Error::NeedMore));
    assert!(
        PrefixedInt::<5>::decode(integer::OVERFLOW)
            == Err(Error::Overflow(Overflow {
                value: (1 << 63) + 30,
                shift: 63,
                byte: 1,
            }))
    );
    let mut bytes = [0x80; 11];
    bytes[0] = 0x1f;
    for terminal in 2..=0x7f {
        bytes[10] = terminal;
        assert!(
            PrefixedInt::<5>::decode(&bytes)
                == Err(Error::Overflow(Overflow {
                    value: 31,
                    shift: 63,
                    byte: terminal,
                })),
            "terminal {terminal:#x} must not lose its high bits at shift 63",
        );
    }
}

#[test]
fn preserves_write_error() {
    let mut bytes = [0xa5; 3];
    let mut output = Slice::new(&mut bytes[..2]);
    let error = PrefixedInt::<5>::new(1337)
        .try_encode(0, &mut output)
        .expect_err("third byte exceeds the output");
    assert_eq!(error.into_parts(), (NonZero::<usize>::MIN, 0));
    assert_eq!(output.finish(), 2);
    assert_eq!(bytes, [31, 154, 0xa5]);
}

fn check<const BITS: u8>(prefix: u8)
where
    PrefixedInt<BITS>: Width,
{
    for &value in integer::VALUES {
        let integer = PrefixedInt::<BITS>::new(value);
        let mut encoded = Vec::new();
        integer.encode(prefix, &mut encoded);
        assert_eq!(integer.encoded_len().get(), encoded.len());
        assert!(PrefixedInt::<BITS>::decode(&encoded) == Ok((integer, integer.encoded_len())));
        assert_eq!(integer.pack::<11>(prefix).as_ref(), encoded);
        let mut bytes = [0; 11];
        let mut output = Slice::new(&mut bytes);
        integer.try_encode(prefix, &mut output).expect("wire bytes");
        let written = output.finish();
        assert_eq!(&bytes[..written], encoded);
        encoded.push(0xa5);
        let mut input = encoded.as_slice();
        assert!(PrefixedInt::<BITS>::take(&mut input) == Ok(integer));
        assert_eq!(input, &[0xa5]);
    }
}
