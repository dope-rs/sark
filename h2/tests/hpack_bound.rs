use common::hpack::{
    self, Fields,
    bound::{Literal, String},
};
pub mod common;

use sark_h2::hpack::{
    decode::{Decoder, Error, limit::Admission},
    encode::Encoder,
};
use sark_http::field::metadata::{integer::PrefixedInt, map::record::Entry, strings::Encoding};

#[test]
fn honors_configured_limit() {
    let block = Fields::new(&hpack::GET).try_encode().expect("header block");
    let mut decoder = Decoder::try_new(4096).expect("decoder");
    decoder.set_max_list(Some(0));
    let mut fields = 0;
    let (mut decoder, admit) = decoder
        .decode_limit(&block, |_, _| fields += 1)
        .expect("bounded block");
    assert_eq!(admit, Admission::Exceeded);
    assert_eq!(fields, 0);
    decoder.set_max_list(None);
    let (_, admit) = decoder
        .decode_limit(&block, |_, _| fields += 1)
        .expect("explicitly unlimited block");
    assert_eq!(admit, Admission::Admitted);
    assert_eq!(fields, hpack::GET.len());
}

#[test]
fn caps_running_total() {
    let mut enc = Encoder::try_new(4096).expect("HPACK encoder capacity");
    let value = vec![b'a'; 4000];
    let header = Entry::new(b"x-bomb", &value);
    let mut block = Vec::new();
    enc.try_encode([header], &mut block).expect("HPACK table");
    let index: u8 = 0x80 | 62;
    for _ in 0..100 {
        block.push(index);
    }

    let mut dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    dec.set_max_list(Some(8192));
    let mut emitted = 0usize;
    let (_, admit) = dec
        .decode_limit(&block, |name, field| {
            assert!(name.is_equal(b"x-bomb"));
            assert!(field.is_equal(&value));
            emitted += 1;
        })
        .expect("bounded indexed fields decode");
    assert!(admit == Admission::Exceeded);
    assert_eq!(emitted, 2);
}

#[test]
fn within_limit_decodes() {
    let mut enc = Encoder::try_new(4096).expect("HPACK encoder capacity");
    let mut block = Vec::new();
    enc.try_encode([Entry::new(b"a", b"b")], &mut block)
        .expect("HPACK table");
    let mut dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    dec.set_max_list(Some(8192));
    let mut emitted = 0usize;
    let (_, admit) = dec
        .decode_limit(&block, |name, value| {
            assert!(name.is_equal(b"a"));
            assert!(value.is_equal(b"b"));
            emitted += 1;
        })
        .expect("bounded literal decodes");
    assert!(admit == Admission::Admitted);
    assert_eq!(emitted, 1);
}

#[test]
fn checks_huffman_tail() {
    let value = vec![b'a'; 32 * 1024];
    let block = Literal::new(b"x", &value)
        .try_encode(false)
        .expect("literal");
    let mut decoder = Decoder::try_new(4096).expect("HPACK decoder capacity");
    decoder.set_max_list(Some(64));

    let mut fields = 0;
    let (decoder, admit) = decoder
        .decode_limit(&block, |_, _| fields += 1)
        .expect("bounded literal decodes");
    assert!(admit == Admission::Exceeded);
    assert_eq!(fields, 0);
    assert_eq!(decoder.dyn_len(), 0);
}

#[test]
fn syncs_indexed_literal() {
    let value = vec![b'a'; 100];
    let mut block = Literal::new(b"x", &value)
        .try_encode(true)
        .expect("literal");
    block.push(0xbe);
    let mut decoder = Decoder::try_new(4096).expect("HPACK decoder capacity");
    decoder.set_max_list(Some(64));

    let mut fields = 0;
    let (decoder, admit) = decoder
        .decode_limit(&block, |_, _| fields += 1)
        .expect("bounded indexed literal decodes");
    assert!(admit == Admission::Exceeded);
    assert_eq!(fields, 0);
    assert_eq!(decoder.dyn_len(), 1);
    let (name, retained) = decoder.dyn_get(0).expect("valid HPACK fixture");
    assert!(name.is_equal(b"x"));
    assert!(retained.is_equal(&value));
}

#[test]
fn rejects_huffman_tail() {
    let mut block = Vec::new();
    block.push(0x00);
    String::new(b"x", Encoding::Plain)
        .try_encode(&mut block)
        .expect("literal");
    PrefixedInt::<7>::new(4).encode(0x80, &mut block);
    block.extend_from_slice(&[0xff; 4]);
    let mut decoder = Decoder::try_new(4096).expect("HPACK decoder capacity");
    decoder.set_max_list(Some(0));

    let mut fields = 0;
    let result = decoder.decode_limit(&block, |_, _| fields += 1);
    assert_eq!(
        result.err().expect("Huffman tail must be rejected"),
        Error::Encoding {
            consumed: 3,
            decoded: 0
        },
    );
    assert_eq!(fields, 0);
}
