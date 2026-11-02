use std::iter;

use common::hpack::{Field, GET, RESIZE_64_128_GET, RESIZE_128_GET};

pub mod common;

use sark_h2::hpack::{
    decode::{Block, Decoder, Error, limit::Admission},
    encode::Encoder,
};
use sark_http::field::metadata::{integer::PrefixedInt, map::record::Entry};

#[test]
fn encoder_emits_update() {
    let mut enc = Encoder::try_new(4096).expect("HPACK encoder capacity");
    enc.set_max_size(1024);
    let mut buf = Vec::new();
    enc.try_encode(iter::empty::<Entry<'_>>(), &mut buf)
        .expect("HPACK table");
    assert_eq!(buf[0] & 0xe0, 0x20);
}

#[test]
fn encoder_reconciles_size() {
    let mut encoder = Encoder::try_new(4096).expect("HPACK encoder capacity");
    encoder.set_max_size(1024);
    encoder.set_max_size(2048);
    let mut block = Vec::new();
    encoder
        .try_encode(iter::empty::<Entry<'_>>(), &mut block)
        .expect("HPACK table");

    let (reset, reset_n) = PrefixedInt::<5>::decode(&block)
        .expect("the first emitted update is a valid five-bit HPACK integer");
    let (size, size_n) = PrefixedInt::<5>::decode(&block[reset_n.get()..])
        .expect("the final emitted update is a valid five-bit HPACK integer");
    assert_eq!(reset.get(), 1024);
    assert_eq!(size.get(), 2048);
    assert_eq!(reset_n.get() + size_n.get(), block.len());
}

#[test]
fn resize_preserves_entries() {
    let mut encoder = Encoder::try_new(4096).expect("encoder");
    let mut block = Vec::new();
    encoder
        .try_encode([GET[3]], &mut block)
        .expect("HPACK table");
    block.clear();
    encoder.set_max_size(1024);
    encoder.set_max_size(2048);
    encoder
        .try_encode([GET[3]], &mut block)
        .expect("HPACK table");
    let (_, first) = PrefixedInt::<5>::decode(&block).expect("minimum");
    let (_, second) = PrefixedInt::<5>::decode(&block[first.get()..]).expect("final");
    assert_eq!(&block[first.get() + second.get()..], &[0xbe]);
}

#[test]
fn encoder_clamps_size() {
    let mut encoder = Encoder::try_new(128).expect("HPACK encoder capacity");
    assert_eq!(encoder.capacity(), 128);
    assert_eq!(encoder.max_size(), 128);

    encoder.set_max_size(16);
    encoder.set_max_size(usize::MAX);
    assert_eq!(encoder.capacity(), 128);
    assert_eq!(encoder.max_size(), 128);

    let mut block = Vec::new();
    encoder
        .try_encode(iter::empty::<Entry<'_>>(), &mut block)
        .expect("HPACK table");
    let (reset, reset_n) = PrefixedInt::<5>::decode(&block)
        .expect("the clamped reset is a valid five-bit HPACK integer");
    let (size, size_n) = PrefixedInt::<5>::decode(&block[reset_n.get()..])
        .expect("the clamped size is a valid five-bit HPACK integer");
    assert_eq!(reset.get(), 16);
    assert_eq!(size.get(), 128);
    assert_eq!(reset_n.get() + size_n.get(), block.len());
}

#[test]
fn decoder_clamps_limit() {
    let mut decoder = Decoder::try_new(128).expect("HPACK decoder capacity");
    decoder.set_max_size(usize::MAX);
    let mut update = Vec::new();
    PrefixedInt::<5>::new(129).encode(0x20, &mut update);
    let mut fields = 0;
    let result = decoder.decode_limit(&update, |_, _| fields += 1);
    assert!(matches!(
        result,
        Err(Error::Resize {
            actual: 129,
            limit: 128
        })
    ));
    assert_eq!(fields, 0, "size update emitted a field");
}

#[test]
fn decoder_rejects_late() {
    let bytes = [0x82, 0x20];
    let decoder = Decoder::try_new(4096).expect("HPACK decoder capacity");
    assert!(matches!(
        decoder.decode_limit(&bytes, |name, value| assert!(
            name.is_equal(b":method") && value.is_equal(b"GET")
        )),
        Err(Error::LateResize)
    ));
}

#[test]
fn fragments_reject_late() {
    let decoder = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let block: Block = Block::from(decoder);
    let block = block
        .decode(&[0x82], &mut |name, value| {
            assert!(name.is_equal(b":method") && value.is_equal(b"GET"))
        })
        .expect("the complete indexed field decodes before the later update");
    let mut fields = 0;
    let result = block.decode(&[0x3f, 0x01], &mut |_, _| fields += 1);
    assert!(matches!(result, Err(Error::LateResize)));
    assert_eq!(fields, 0, "late size update emitted a field");
}

#[test]
fn accepts_leading_updates() {
    let bytes = [0x20, 0x3f, 0x01, 0x82];
    let decoder = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let mut emitted = Vec::new();
    let (decoder, admission) = decoder
        .decode_limit(&bytes, |name, value| {
            emitted.push(Field::new(name, value).into_parts());
        })
        .expect("leading size updates followed by one indexed field decode");
    assert_eq!(admission, Admission::Admitted);
    assert_eq!(decoder.dyn_max(), 32);
    assert_eq!(emitted, [(b":method".to_vec(), b"GET".to_vec())]);
}

#[test]
fn decoder_requires_minimum() {
    let mut decoder = Decoder::try_new(4096).expect("decoder");
    decoder.set_max_size(64);
    decoder.set_max_size(128);
    assert!(matches!(
        decoder.decode_limit(RESIZE_128_GET, |_, _| {}),
        Err(Error::Resize {
            actual: 128,
            limit: 64
        })
    ));

    let mut decoder = Decoder::try_new(4096).expect("decoder");
    decoder.set_max_size(64);
    decoder.set_max_size(128);
    let (decoder, admission) = decoder
        .decode_limit(RESIZE_64_128_GET, |name, value| {
            assert!(name.is_equal(b":method") && value.is_equal(b"GET"))
        })
        .expect("minimum then final update");
    assert_eq!(admission, Admission::Admitted);
    assert_eq!(decoder.dyn_max(), 128);
}

#[test]
fn empty_requires_update() {
    let mut decoder = Decoder::try_new(4096).expect("decoder");
    decoder.set_max_size(64);
    assert!(matches!(
        decoder.decode_limit(&[], |_, _| {}),
        Err(Error::MissingResize { limit: 64 })
    ));
    let mut decoder = Decoder::try_new(4096).expect("decoder");
    decoder.set_max_size(64);
    assert!(matches!(
        Block::from(decoder).finish(),
        Err(Error::MissingResize { limit: 64 })
    ));
}
