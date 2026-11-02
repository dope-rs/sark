use common::hpack::Encoded;
pub mod common;

use sark_h2::hpack::decode::Decoder;

#[test]
fn decodes_sequence() {
    let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");

    let r1: &[u8] = &[
        0x82, 0x86, 0x84, 0x41, 0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90,
        0xf4, 0xff,
    ];
    let (dec, got) = Encoded::new(r1)
        .try_collect(dec)
        .expect("valid HPACK fixture");
    assert_eq!(
        got,
        vec![
            (b":method".to_vec(), b"GET".to_vec()),
            (b":scheme".to_vec(), b"http".to_vec()),
            (b":path".to_vec(), b"/".to_vec()),
            (b":authority".to_vec(), b"www.example.com".to_vec()),
        ]
    );
    assert_eq!(dec.dyn_size(), 57);

    let r2: &[u8] = &[
        0x82, 0x86, 0x84, 0xbe, 0x58, 0x86, 0xa8, 0xeb, 0x10, 0x64, 0x9c, 0xbf,
    ];
    let (dec, got) = Encoded::new(r2)
        .try_collect(dec)
        .expect("valid HPACK fixture");
    assert_eq!(
        got,
        vec![
            (b":method".to_vec(), b"GET".to_vec()),
            (b":scheme".to_vec(), b"http".to_vec()),
            (b":path".to_vec(), b"/".to_vec()),
            (b":authority".to_vec(), b"www.example.com".to_vec()),
            (b"cache-control".to_vec(), b"no-cache".to_vec()),
        ]
    );
    assert_eq!(dec.dyn_size(), 110);

    let r3: &[u8] = &[
        0x82, 0x87, 0x85, 0xbf, 0x40, 0x88, 0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xa9, 0x7d, 0x7f, 0x89,
        0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xb8, 0xe8, 0xb4, 0xbf,
    ];
    let (dec, got) = Encoded::new(r3)
        .try_collect(dec)
        .expect("valid HPACK fixture");
    assert_eq!(
        got,
        vec![
            (b":method".to_vec(), b"GET".to_vec()),
            (b":scheme".to_vec(), b"https".to_vec()),
            (b":path".to_vec(), b"/index.html".to_vec()),
            (b":authority".to_vec(), b"www.example.com".to_vec()),
            (b"custom-key".to_vec(), b"custom-value".to_vec()),
        ]
    );
    assert_eq!(dec.dyn_size(), 164);
}
