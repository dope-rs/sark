use common::hpack::Encoded;
pub mod common;

use sark_h2::hpack::decode::Decoder;

#[test]
fn evicts_at_256() {
    let dec = Decoder::try_new(256).expect("HPACK decoder capacity");

    let r1: &[u8] = &[
        0x48, 0x03, b'3', b'0', b'2', 0x58, 0x07, b'p', b'r', b'i', b'v', b'a', b't', b'e', 0x61,
        0x1d, b'M', b'o', b'n', b',', b' ', b'2', b'1', b' ', b'O', b'c', b't', b' ', b'2', b'0',
        b'1', b'3', b' ', b'2', b'0', b':', b'1', b'3', b':', b'2', b'1', b' ', b'G', b'M', b'T',
        0x6e, 0x17, b'h', b't', b't', b'p', b's', b':', b'/', b'/', b'w', b'w', b'w', b'.', b'e',
        b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c', b'o', b'm',
    ];
    let (dec, got) = Encoded::new(r1)
        .try_collect(dec)
        .expect("valid HPACK fixture");
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
    assert_eq!(dec.dyn_len(), 4);

    let r2: &[u8] = &[0x48, 0x03, b'3', b'0', b'7', 0xc1, 0xc0, 0xbf];
    let (dec, got) = Encoded::new(r2)
        .try_collect(dec)
        .expect("valid HPACK fixture");
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
    assert_eq!(dec.dyn_len(), 4);

    let r3: &[u8] = &[
        0x88, 0xc1, 0x61, 0x1d, b'M', b'o', b'n', b',', b' ', b'2', b'1', b' ', b'O', b'c', b't',
        b' ', b'2', b'0', b'1', b'3', b' ', b'2', b'0', b':', b'1', b'3', b':', b'2', b'2', b' ',
        b'G', b'M', b'T', 0xc0, 0x5a, 0x04, b'g', b'z', b'i', b'p', 0x77, 0x38, b'f', b'o', b'o',
        b'=', b'A', b'S', b'D', b'J', b'K', b'H', b'Q', b'K', b'B', b'Z', b'X', b'O', b'Q', b'W',
        b'E', b'O', b'P', b'I', b'U', b'A', b'X', b'Q', b'W', b'E', b'O', b'I', b'U', b';', b' ',
        b'm', b'a', b'x', b'-', b'a', b'g', b'e', b'=', b'3', b'6', b'0', b'0', b';', b' ', b'v',
        b'e', b'r', b's', b'i', b'o', b'n', b'=', b'1',
    ];
    let (dec, got) = Encoded::new(r3)
        .try_collect(dec)
        .expect("valid HPACK fixture");
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
    assert_eq!(dec.dyn_len(), 3);
}
