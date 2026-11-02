use sark_http::field::base64;
use sark_ws::upgrade::{Key, KeyError};

#[test]
fn accept_matches_rfc6455() {
    let key = Key::from_nonce(*b"the sample nonce");
    assert_eq!(key.as_bytes(), b"dGhlIHNhbXBsZSBub25jZQ==");
    assert_eq!(key.accept(), *b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
}

#[test]
fn parses_only_canonical() {
    (Key::parse(b"dGhlIHNhbXBsZSBub25jZQ=="))
        .as_ref()
        .expect("test operation must succeed");
    assert!(matches!(
        Key::parse(&[b'A'; 24]),
        Err(KeyError::Decoded { actual: 18 })
    ));
    assert!(
        Key::parse(b"dGhlIHNhbXBsZSBub25jZ!==")
            == Err(KeyError::Base64(base64::Error::Byte {
                index: 21,
                byte: b'!',
            }))
    );
}
