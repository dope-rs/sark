use common::hpack::Encoded;
pub mod common;

use sark_h2::hpack::decode::Decoder;

#[test]
fn decodes_sequence() {
    let dec = Decoder::try_new(4096).expect("HPACK decoder capacity");

    let r1: &[u8] = &[
        0x82, 0x86, 0x84, 0x41, 0x0f, b'w', b'w', b'w', b'.', b'e', b'x', b'a', b'm', b'p', b'l',
        b'e', b'.', b'c', b'o', b'm',
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
    assert_eq!(dec.dyn_len(), 1);
    assert_eq!(dec.dyn_size(), 57);
    let (n, v) = dec.dyn_get(0).expect("valid HPACK fixture");
    assert!(n.is_equal(b":authority"));
    assert!(v.is_equal(b"www.example.com"));

    let r2: &[u8] = &[
        0x82, 0x86, 0x84, 0xbe, 0x58, 0x08, b'n', b'o', b'-', b'c', b'a', b'c', b'h', b'e',
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
    assert_eq!(dec.dyn_len(), 2);
    assert_eq!(dec.dyn_size(), 110);

    let r3: &[u8] = &[
        0x82, 0x87, 0x85, 0xbf, 0x40, 0x0a, b'c', b'u', b's', b't', b'o', b'm', b'-', b'k', b'e',
        b'y', 0x0c, b'c', b'u', b's', b't', b'o', b'm', b'-', b'v', b'a', b'l', b'u', b'e',
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
    assert_eq!(dec.dyn_len(), 3);
    assert_eq!(dec.dyn_size(), 164);
}
