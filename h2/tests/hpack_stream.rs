use std::cell::Cell;

use common::hpack::stream::{Decoded, GET_BAD_INDEX, GET_PATH, Repeated};
pub mod common;

use sark_h2::hpack::decode::{Block, Decoder, Error, limit::Admission};
use sark_http::field::metadata::map::record::Span;

#[test]
fn all_split_points() {
    let encoded = Repeated::default().try_encode().expect("HPACK fixture");
    let chunks = (0..=encoded.len())
        .map(|split| vec![&encoded[..split], &encoded[split..]])
        .chain([encoded.chunks(1).collect()]);
    for chunks in chunks {
        let decoder = Decoder::try_new(4096).expect("HPACK decoder capacity");
        let Decoded {
            fields,
            decoder,
            admit,
        } = Decoded::try_chunks(chunks, decoder).expect("valid HPACK fixture");
        assert!(admit == Admission::Admitted);
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], (b":method".to_vec(), b"GET".to_vec()));
        assert_eq!(fields[1].0, [b'z'; 160]);
        assert_eq!(fields[1].1, [b'a'; 300]);
        assert_eq!(fields[2], fields[1]);
        assert_eq!(decoder.dyn_max(), 1024);
        assert_eq!(decoder.dyn_len(), 1);
        assert!(matches!(
            decoder.dyn_get(0),
            Some((name, value))
                if name.is_equal(&[b'z'; 160]) && value.is_equal(&[b'a'; 300])
        ));
    }
}

#[test]
fn resumes_next_prefix() {
    let decoder = Decoder::try_new(4096).expect("decoder");
    let Decoded { fields, .. } =
        Decoded::try_chunks([&GET_PATH[..2], &GET_PATH[2..]], decoder).expect("split literal");
    assert_eq!(
        fields,
        [
            (b":method".to_vec(), b"GET".to_vec()),
            (b":path".to_vec(), b"/".to_vec()),
        ]
    );
}

#[test]
fn emits_prefix_on_error() {
    for split in [
        None,
        Some(GET_BAD_INDEX.len()),
        Some(GET_BAD_INDEX.len() - 1),
    ] {
        let decoder = Decoder::try_new(4096).expect("decoder");
        let emitted = Cell::new(0);
        let mut emit = |name: Span<'_>, value: Span<'_>| {
            assert!(name.is_equal(b":method") && value.is_equal(b"GET"));
            emitted.set(emitted.get() + 1);
        };
        let error = match split {
            None => decoder.decode_limit(GET_BAD_INDEX, &mut emit).err(),
            Some(split) if split == GET_BAD_INDEX.len() => {
                Block::from(decoder).decode(GET_BAD_INDEX, &mut emit).err()
            }
            Some(split) => {
                let block = Block::from(decoder)
                    .decode(&GET_BAD_INDEX[..split], &mut emit)
                    .expect("incomplete suffix");
                assert_eq!(emitted.get(), 1);
                block.decode(&GET_BAD_INDEX[split..], &mut emit).err()
            }
        };
        assert_eq!(error, Some(Error::BadIndex));
        assert_eq!(emitted.get(), 1);
    }
}

#[test]
fn rejects_split_huffman() {
    let encoded = [0x00, 0x01, b'x', 0x84, 0xff, 0xff, 0xff, 0xff];
    for split in 0..=encoded.len() {
        let decoder = Decoder::try_new(4096).expect("HPACK decoder capacity");
        let block = Block::from(decoder);
        let mut fields = 0;
        let result = block
            .decode(&encoded[..split], &mut |_: Span<'_>, _: Span<'_>| {
                fields += 1
            })
            .and_then(|block| {
                block.decode(&encoded[split..], &mut |_: Span<'_>, _: Span<'_>| {
                    fields += 1
                })
            })
            .and_then(|block| {
                block.finish().map(|(decoder, admit)| {
                    assert!(admit == Admission::Admitted);
                    assert_eq!(decoder.dyn_len(), 0);
                })
            });
        assert_eq!(
            result,
            Err(Error::Encoding {
                consumed: if split == encoded.len() {
                    3
                } else {
                    7 - split.max(4)
                },
                decoded: 0,
            }),
            "split at {split}",
        );
        assert_eq!(fields, 0);
    }
}

#[test]
fn syncs_over() {
    let encoded = Repeated::default().try_encode().expect("HPACK fixture");
    for split in 0..=encoded.len() {
        let mut decoder = Decoder::try_new(4096).expect("HPACK decoder capacity");
        decoder.set_max_list(Some(64));
        let block = Block::from(decoder);
        let mut emitted = 0;
        let block = block
            .decode(&encoded[..split], &mut |name: Span<'_>, value: Span<'_>| {
                assert!(name.is_equal(b":method"));
                assert!(value.is_equal(b"GET"));
                emitted += 1;
            })
            .expect("valid HPACK fixture");
        let block = block
            .decode(&encoded[split..], &mut |name: Span<'_>, value: Span<'_>| {
                assert!(name.is_equal(b":method"));
                assert!(value.is_equal(b"GET"));
                emitted += 1;
            })
            .expect("valid HPACK fixture");
        let (decoder, admit) = block.finish().expect("valid HPACK fixture");
        assert!(admit == Admission::Exceeded);
        assert_eq!(emitted, 1);
        assert_eq!(decoder.dyn_len(), 1);
        assert!(matches!(
            decoder.dyn_get(0),
            Some((name, value))
                if name.is_equal(&[b'z'; 160]) && value.is_equal(&[b'a'; 300])
        ));
    }
}

#[test]
fn rejects_incomplete() {
    for encoded in [
        &[0xff][..],
        &[0x40][..],
        &[0x40, 0x03, b'a'][..],
        &[0x40, 0x01, b'a'][..],
        &[0x40, 0x01, b'a', 0x03, b'b'][..],
        &[0x40, 0x01, b'a', 0x81][..],
    ] {
        let decoder = Decoder::try_new(4096).expect("HPACK decoder capacity");
        let block = Block::from(decoder);
        let mut fields = 0;
        let block = block
            .decode(encoded, &mut |_: Span<'_>, _: Span<'_>| fields += 1)
            .expect("valid HPACK fixture");
        let result = block.finish();
        assert!(matches!(result, Err(Error::NeedMore)), "{encoded:02x?}");
        assert_eq!(fields, 0);
    }
}

#[test]
fn rejects_split_overflow() {
    let mut encoded = [0x80; 11];
    encoded[0] = 0x3f;
    encoded[10] = 0x02;
    for split in 0..=encoded.len() {
        let decoder = Decoder::try_new(4096).expect("HPACK decoder capacity");
        let block: Block = Block::from(decoder);
        let mut fields = 0;
        let mut emit = |_: Span<'_>, _: Span<'_>| fields += 1;
        let result = block
            .decode(&encoded[..split], &mut emit)
            .and_then(|block| block.decode(&encoded[split..], &mut emit));
        assert!(
            matches!(
                result,
                Err(Error::BadInteger(
                    sark_http::field::metadata::integer::Overflow {
                        value: 31,
                        shift: 63,
                        byte: 2
                    }
                ))
            ),
            "split at {split}"
        );
        assert_eq!(fields, 0);
    }
}
