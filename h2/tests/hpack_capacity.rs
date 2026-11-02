use common::hpack::{Encoded, bound::Literal};
pub mod common;

use sark_h2::hpack::{
    capacity::Error,
    decode::{Block, Decoder, limit::Admission},
    encode::Encoder,
};
use sark_http::{
    field::metadata::map::record::{Entry, Span},
    payload::compress::dynamic::plan,
};

#[test]
fn rejects_encoder_limit() {
    assert!(matches!(
        Encoder::try_new(usize::MAX),
        Err(Error::Plan(plan::Error::Capacity {
            actual: usize::MAX,
            maximum,
        })) if maximum == u32::MAX as usize
    ));
}

#[test]
fn rejects_decoder_limit() {
    assert!(matches!(
        Decoder::try_new(usize::MAX),
        Err(Error::Plan(plan::Error::Capacity {
            actual: usize::MAX,
            maximum,
        })) if maximum == u32::MAX as usize
    ));
}

#[test]
fn zero_disables_tables() {
    let mut encoder = Encoder::try_new(0).expect("zero-capacity encoder");
    let decoder = Decoder::try_new(0).expect("zero-capacity decoder");
    let mut block = Vec::new();

    encoder
        .try_encode([Entry::new(b"x", b"y")], &mut block)
        .expect("HPACK table");
    let (decoder, fields) = Encoded::new(&block)
        .try_collect(decoder)
        .expect("zero-capacity literal decodes");
    assert_eq!(fields, vec![(b"x".to_vec(), b"y".to_vec())]);

    assert_eq!(encoder.capacity(), 0);
    assert_eq!(decoder.dyn_len(), 0);
}

#[test]
fn clears_oversized_entry() {
    for (name, value) in [
        (b"long-name".as_slice(), b"v".as_slice()),
        (b"n", b"long-value"),
    ] {
        for encoded in [
            Literal::new(name, value).try_encode(true).expect("literal"),
            Literal::new(name, value).encode_plain(true),
        ] {
            for capacity in [0, 34] {
                for chunk in [1, encoded.len()] {
                    let decoder = Decoder::try_new(capacity).expect("bounded decoder");
                    let (decoder, admission) = decoder
                        .decode_limit(&Literal::new(b"x", b"y").encode_plain(true), |_, _| {})
                        .expect("initial table entry");
                    assert_eq!(admission, Admission::Admitted);
                    let mut fields = 0;
                    let mut observe = |key: Span<'_>, data: Span<'_>| {
                        assert!(key.is_equal(name));
                        assert!(data.is_equal(value));
                        fields += 1;
                    };
                    let (decoder, admission) = if chunk == encoded.len() {
                        decoder
                            .decode_limit(&encoded, &mut observe)
                            .expect("complete field")
                    } else {
                        let mut block = Block::from(decoder);
                        for bytes in encoded.chunks(chunk) {
                            block = block.decode(bytes, &mut observe).expect("fragmented field");
                        }
                        block.finish().expect("complete field")
                    };
                    assert_eq!(admission, Admission::Admitted);
                    assert_eq!(fields, 1);
                    assert_eq!(decoder.dyn_len(), 0);
                }
            }
        }
    }
}
