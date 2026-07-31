use std::mem::size_of;

use sark_core::http::huffman::CODES;
use sark_core::http::{
    HpackHuffmanDecodeError, HpackHuffmanDecoder, HpackHuffmanEncoded, HpackHuffmanError,
    HpackHuffmanSource,
};

fn reference_decode(input: &[u8]) -> Result<Vec<u8>, HpackHuffmanError> {
    let mut out = Vec::new();
    let mut prefix = 0u32;
    let mut prefix_len = 0u8;
    for &byte in input {
        let mut remaining = 8u8;
        while remaining != 0 {
            remaining -= 1;
            prefix = (prefix << 1) | u32::from((byte >> remaining) & 1);
            prefix_len += 1;

            let mut is_prefix = false;
            let mut decoded = None;
            for (symbol, &(code, code_len)) in CODES.iter().enumerate() {
                if code_len == prefix_len && code == prefix {
                    decoded = Some(symbol);
                    break;
                }
                if code_len > prefix_len && code >> (code_len - prefix_len) == prefix {
                    is_prefix = true;
                }
            }
            if let Some(symbol) = decoded {
                if symbol == 256 {
                    return Err(HpackHuffmanError);
                }
                out.push(symbol as u8);
                prefix = 0;
                prefix_len = 0;
            } else if !is_prefix {
                return Err(HpackHuffmanError);
            }
        }
    }
    if prefix_len == 0 || (prefix_len <= 7 && prefix == (1u32 << prefix_len) - 1) {
        Ok(out)
    } else {
        Err(HpackHuffmanError)
    }
}

fn decode(input: &[u8]) -> Result<Vec<u8>, HpackHuffmanError> {
    let mut out = Vec::new();
    HpackHuffmanEncoded::new(input).decode(&mut out)?;
    Ok(out)
}

#[test]
fn nominal_views_are_slice_layouts() {
    assert_eq!(size_of::<HpackHuffmanSource<'_>>(), size_of::<&[u8]>());
    assert_eq!(size_of::<HpackHuffmanEncoded<'_>>(), size_of::<&[u8]>());
    assert_eq!(size_of::<HpackHuffmanDecoder>(), 2);
}

#[test]
fn rfc_7541_authority_vector() {
    let raw = b"www.example.com";
    let expected = [
        0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
    ];
    let source = HpackHuffmanSource::new(raw);
    let mut encoded = Vec::new();
    source.encode(&mut encoded);
    assert_eq!(source.encoded_len(), expected.len());
    assert_eq!(encoded, expected);
    assert_eq!(decode(&encoded).unwrap(), raw);
}

#[test]
fn every_symbol_round_trips() {
    let raw: Vec<u8> = (0..=u8::MAX).collect();
    let source = HpackHuffmanSource::new(&raw);
    let mut encoded = Vec::new();
    source.encode(&mut encoded);
    assert_eq!(source.encoded_len(), encoded.len());
    assert_eq!(decode(&encoded).unwrap(), raw);
}

#[test]
fn every_byte_boundary_preserves_incremental_decoder_state() {
    let raw: Vec<u8> = (0..=u8::MAX).collect();
    let source = HpackHuffmanSource::new(&raw);
    let mut encoded = Vec::new();
    source.encode(&mut encoded);

    for split in 0..=encoded.len() {
        let mut decoder = HpackHuffmanDecoder::new();
        let mut decoded = Vec::new();
        decoder
            .feed(&encoded[..split], |byte| {
                decoded.push(byte);
                Ok::<_, std::convert::Infallible>(())
            })
            .unwrap();
        decoder
            .feed(&encoded[split..], |byte| {
                decoded.push(byte);
                Ok::<_, std::convert::Infallible>(())
            })
            .unwrap();
        decoder.finish().unwrap();
        assert_eq!(decoded, raw, "split at encoded byte {split}");
    }
}

#[test]
fn eos_and_invalid_padding_are_rejected() {
    for encoded in [
        &[0xff][..],
        &[0x00][..],
        &[0xff, 0xff, 0xff, 0xfc][..],
        &[0xff, 0xff, 0xff, 0xff][..],
    ] {
        assert_eq!(decode(encoded), Err(HpackHuffmanError));
    }
}

#[test]
fn sink_error_is_distinct_from_invalid_encoding() {
    let mut encoded = Vec::new();
    HpackHuffmanSource::new(b"abc").encode(&mut encoded);
    let result = HpackHuffmanEncoded::new(&encoded).decode_with(|_| Err::<(), _>("full"));
    assert_eq!(result, Err(HpackHuffmanDecodeError::Sink("full")));
}

#[test]
fn generated_dfa_matches_independent_bit_decoder() {
    for byte in 0..=u8::MAX {
        let input = [byte];
        assert_eq!(decode(&input), reference_decode(&input), "{input:02x?}");
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
        assert_eq!(
            decode(&input[..len]),
            reference_decode(&input[..len]),
            "{:02x?}",
            &input[..len]
        );
    }
}
