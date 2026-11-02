use common::hpack::Encoded;
pub mod common;

use sark_h2::hpack::decode::Decoder;

#[test]
fn evicts_at_256() {
    let dec = Decoder::try_new(256).expect("HPACK decoder capacity");

    let r1: &[u8] = &[
        0x48, 0x82, 0x64, 0x02, 0x58, 0x85, 0xae, 0xc3, 0x77, 0x1a, 0x4b, 0x61, 0x96, 0xd0, 0x7a,
        0xbe, 0x94, 0x10, 0x54, 0xd4, 0x44, 0xa8, 0x20, 0x05, 0x95, 0x04, 0x0b, 0x81, 0x66, 0xe0,
        0x82, 0xa6, 0x2d, 0x1b, 0xff, 0x6e, 0x91, 0x9d, 0x29, 0xad, 0x17, 0x18, 0x63, 0xc7, 0x8f,
        0x0b, 0x97, 0xc8, 0xe9, 0xae, 0x82, 0xae, 0x43, 0xd3,
    ];
    let (dec, got) = Encoded::new(r1)
        .try_collect(dec)
        .expect("RFC 7541 C.6.1 response decodes");
    assert_eq!(
        got,
        vec![
            (b":status".to_vec(), b"302".to_vec()),
            (b"cache-control".to_vec(), b"private".to_vec()),
            (b"date".to_vec(), b"Mon, 21 Oct 2013 20:13:21 GMT".to_vec()),
            (b"location".to_vec(), b"https://www.example.com".to_vec()),
        ]
    );
    assert_eq!(dec.dyn_size(), 222);

    let r2: &[u8] = &[0x48, 0x83, 0x64, 0x0e, 0xff, 0xc1, 0xc0, 0xbf];
    let (dec, got) = Encoded::new(r2)
        .try_collect(dec)
        .expect("RFC 7541 C.6.2 response decodes");
    assert_eq!(
        got,
        vec![
            (b":status".to_vec(), b"307".to_vec()),
            (b"cache-control".to_vec(), b"private".to_vec()),
            (b"date".to_vec(), b"Mon, 21 Oct 2013 20:13:21 GMT".to_vec()),
            (b"location".to_vec(), b"https://www.example.com".to_vec()),
        ]
    );
    assert_eq!(dec.dyn_size(), 222);

    let r3: &[u8] = &[
        0x88, 0xc1, 0x61, 0x96, 0xd0, 0x7a, 0xbe, 0x94, 0x10, 0x54, 0xd4, 0x44, 0xa8, 0x20, 0x05,
        0x95, 0x04, 0x0b, 0x81, 0x66, 0xe0, 0x84, 0xa6, 0x2d, 0x1b, 0xff, 0xc0, 0x5a, 0x83, 0x9b,
        0xd9, 0xab, 0x77, 0xad, 0x94, 0xe7, 0x82, 0x1d, 0xd7, 0xf2, 0xe6, 0xc7, 0xb3, 0x35, 0xdf,
        0xdf, 0xcd, 0x5b, 0x39, 0x60, 0xd5, 0xaf, 0x27, 0x08, 0x7f, 0x36, 0x72, 0xc1, 0xab, 0x27,
        0x0f, 0xb5, 0x29, 0x1f, 0x95, 0x87, 0x31, 0x60, 0x65, 0xc0, 0x03, 0xed, 0x4e, 0xe5, 0xb1,
        0x06, 0x3d, 0x50, 0x07,
    ];
    let (dec, got) = Encoded::new(r3)
        .try_collect(dec)
        .expect("RFC 7541 C.6.3 response decodes");
    assert_eq!(
        got,
        vec![
            (b":status".to_vec(), b"200".to_vec()),
            (b"cache-control".to_vec(), b"private".to_vec()),
            (b"date".to_vec(), b"Mon, 21 Oct 2013 20:13:22 GMT".to_vec()),
            (b"location".to_vec(), b"https://www.example.com".to_vec()),
            (b"content-encoding".to_vec(), b"gzip".to_vec()),
            (
                b"set-cookie".to_vec(),
                b"foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1".to_vec()
            ),
        ]
    );
    assert_eq!(dec.dyn_size(), 215);
}
