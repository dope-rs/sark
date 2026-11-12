pub mod common;
use o3::buffer::storage::Shared;
use sark_json::writing::TryEncode;

use crate::common::values::*;

#[test]
fn maps_nonfinite_to_null() {
    let output = Item {
        score: f64::INFINITY,
        error: sark_json::Token::new().into(),
    };
    let encoded = output.try_encode().expect("encode");
    assert_eq!(encoded.as_slice(), br#"{"score":null,"error":""}"#);
}

#[test]
fn encodes_borrowed_bytes() {
    let value = BorrowedText {
        value: sark_json::Text::from("driver-owned"),
        raw: sark_json::Raw::try_from(sark_json::Text::from(RAW_TEXT)).expect("raw"),
    };
    let encoded = value.try_encode().expect("encode");
    assert_eq!(
        encoded.as_slice(),
        r#"{"value":"driver-owned","raw":{"é":["한","\uD83D\uDE00"]}}"#.as_bytes()
    );
}

#[test]
fn encodes_optional_fields() {
    let shared = Shared::try_copy(b"a\"b").expect("optional owned text fixture");
    let raw = Shared::try_copy(b"17").expect("optional raw fixture");
    let plain = Shared::try_copy(b"plain").expect("optional plain fixture");
    let retained = shared.try_clone().expect("optional retained text fixture");
    let present = Sparse {
        count: Some(7),
        delta: Some(-3),
        score: Some(1.25),
        ok: Some(true),
        text: Text {
            name: Some(String::from("x\"y")),
            shared: Some(
                sark_json::Text::try_new(shared)
                    .map_err(|error| error.discard())
                    .expect("text"),
            ),
            retained: Some(
                sark_json::Text::try_new(retained)
                    .map_err(|error| error.discard())
                    .expect("text"),
            ),
            raw: Some(
                sark_json::Raw::try_new(raw)
                    .map_err(|error| error.discard())
                    .expect("raw"),
            ),
            plain: Some(
                sark_json::Plain::try_new(plain)
                    .map_err(|error| error.discard())
                    .expect("plain"),
            ),
        },
    };
    let encoded = present.try_encode().expect("encode");
    assert_eq!(
        encoded.as_slice(),
        br#"{"count":7,"delta":-3,"score":1.25,"ok":true,"text":{"name":"x\"y","shared":"a\"b","retained":"a\"b","raw":17,"plain":"plain"}}"#
    );

    let absent = Sparse {
        count: None,
        delta: None,
        score: None,
        ok: None,
        text: Text {
            name: None,
            shared: None,
            retained: None,
            raw: None,
            plain: None,
        },
    };
    let encoded = absent.try_encode().expect("encode");
    assert_eq!(
        encoded.as_slice(),
        br#"{"count":null,"delta":null,"score":null,"ok":null,"text":{"name":null,"shared":null,"retained":null,"raw":null,"plain":null}}"#
    );
}
