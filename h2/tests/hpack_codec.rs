use common::hpack::{Encoded, SHORTEST_LITERALS};
pub mod common;

use sark_h2::hpack::{
    decode::Decoder,
    encode::{Encoder, Mode},
};
use sark_http::field::metadata::map::record::Entry;

#[test]
fn shortest_literals() {
    let mut encoder = Encoder::try_new(0).expect("encoder");
    for (field, expected) in SHORTEST_LITERALS {
        let mut output = Vec::new();
        encoder.try_encode([field], &mut output).expect("literal");
        assert_eq!(output, expected);
    }
}

#[test]
fn roundtrips_fields() {
    let mut enc = Encoder::try_new(4096).expect("HPACK encoder capacity");
    let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let headers = [
        Entry::new(b":method", b"POST"),
        Entry::new(b":path", b"/api/v1/x"),
        Entry::new(b":scheme", b"https"),
        Entry::new(b":authority", b"example.com"),
        Entry::new(b"content-type", b"application/json"),
        Entry::new(b"accept", b"text/plain"),
    ];
    let mut buf = Vec::new();
    enc.try_encode(headers.iter().copied(), &mut buf)
        .expect("HPACK table");
    let (_, got) = Encoded::new(&buf)
        .try_collect(dec)
        .expect("valid HPACK fixture");
    let expected: Vec<(Vec<u8>, Vec<u8>)> = headers
        .iter()
        .map(|h| (h.name.to_vec(), h.value.to_vec()))
        .collect();
    assert_eq!(got, expected);
}

#[test]
fn roundtrips_plain() {
    let mut enc = Encoder::try_new(4096).expect("HPACK encoder capacity");
    enc.set_mode(Mode::Plain);
    let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let headers = [Entry::new(b"x-custom", b"hello world")];
    let mut buf = Vec::new();
    enc.try_encode(headers.iter().copied(), &mut buf)
        .expect("HPACK table");
    let (_, got) = Encoded::new(&buf)
        .try_collect(dec)
        .expect("valid HPACK fixture");
    assert_eq!(got, vec![(b"x-custom".to_vec(), b"hello world".to_vec())]);
}

#[test]
fn reuses_dynamic_table() {
    let mut enc = Encoder::try_new(4096).expect("HPACK encoder capacity");
    let mut dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    for _ in 0..5 {
        let headers = [
            Entry::new(b":method", b"GET"),
            Entry::new(b"user-agent", b"sark/1.0"),
        ];
        let mut buf = Vec::new();
        enc.try_encode(headers.iter().copied(), &mut buf)
            .expect("HPACK table");
        let decoded = Encoded::new(&buf)
            .try_collect(dec)
            .expect("valid HPACK fixture");
        dec = decoded.0;
        let got = decoded.1;
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, b":method");
        assert_eq!(got[1].0, b"user-agent");
        assert_eq!(got[1].1, b"sark/1.0");
    }
    assert!(dec.dyn_len() >= 1);
}

#[test]
fn keeps_tables_aligned() {
    let mut enc = Encoder::try_new(68).expect("HPACK encoder capacity");
    enc.set_mode(Mode::Plain);
    let mut dec = Decoder::try_new(68).expect("HPACK decoder capacity");
    let initial = [
        Entry::new(b"a", b"1"),
        Entry::new(b"b", b"2"),
        Entry::new(b"c", b"3"),
    ];
    let mut buf = Vec::new();
    enc.try_encode(initial, &mut buf).expect("HPACK table");
    let decoded = Encoded::new(&buf)
        .try_collect(dec)
        .expect("valid HPACK fixture");
    dec = decoded.0;
    assert_eq!(
        decoded.1,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]
    );

    buf.clear();
    enc.try_encode([Entry::new(b"b", b"2")], &mut buf)
        .expect("HPACK table");
    let (_, fields) = Encoded::new(&buf)
        .try_collect(dec)
        .expect("valid HPACK fixture");
    assert_eq!(fields, vec![(b"b".to_vec(), b"2".to_vec())]);
}
