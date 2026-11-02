use common::hpack::Encoded;
pub mod common;

use sark_h2::hpack::decode::Decoder;

#[test]
fn evicts_oldest() {
    let dec = Decoder::try_new(80).expect("HPACK decoder capacity");
    let bytes: &[u8] = &[
        0x40, 0x03, b'a', b'a', b'a', 0x03, b'A', b'A', b'A', 0x40, 0x03, b'b', b'b', b'b', 0x03,
        b'B', b'B', b'B',
    ];
    let (dec, got) = Encoded::new(bytes)
        .try_collect(dec)
        .expect("valid HPACK fixture");
    assert_eq!(
        got,
        vec![
            (b"aaa".to_vec(), b"AAA".to_vec()),
            (b"bbb".to_vec(), b"BBB".to_vec()),
        ]
    );
    assert_eq!(dec.dyn_len(), 2);

    let more: &[u8] = &[0x40, 0x03, b'c', b'c', b'c', 0x03, b'C', b'C', b'C'];
    let (dec, got) = Encoded::new(more)
        .try_collect(dec)
        .expect("valid HPACK fixture");
    assert_eq!(got, vec![(b"ccc".to_vec(), b"CCC".to_vec())]);
    assert_eq!(dec.dyn_len(), 2);
    let (n0, _) = dec.dyn_get(0).expect("valid HPACK fixture");
    let (n1, _) = dec.dyn_get(1).expect("valid HPACK fixture");
    assert!(n0.is_equal(b"ccc"));
    assert!(n1.is_equal(b"bbb"));
}

#[test]
fn zero_update_clears() {
    let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");
    let bytes: &[u8] = &[0x40, 0x03, b'a', b'a', b'a', 0x03, b'A', b'A', b'A'];
    let (dec, fields) = Encoded::new(bytes)
        .try_collect(dec)
        .expect("indexed literal decodes");
    assert_eq!(fields, vec![(b"aaa".to_vec(), b"AAA".to_vec())]);
    assert_eq!(dec.dyn_len(), 1);
    let upd: &[u8] = &[0x20];
    let (dec, fields) = Encoded::new(upd)
        .try_collect(dec)
        .expect("zero table update decodes");
    assert!(fields.is_empty());
    assert_eq!(dec.dyn_len(), 0);
    assert_eq!(dec.dyn_max(), 0);
}
