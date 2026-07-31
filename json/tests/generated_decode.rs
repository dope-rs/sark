extern crate self as sark;

use o3::buffer::{Bytes, Retained, Shared};
use sark_json::{JsonDecode, JsonRequestDecode};

pub mod json {
    pub use sark_json::*;
}

pub mod sark_core {
    pub use ::sark_core::*;
}

#[sark_gen::json(ordered)]
struct General {
    text: Bytes<Retained>,
    #[field(raw)]
    number: Bytes<Retained>,
}

#[sark_gen::json(ordered, plain)]
struct Plain {
    text: Bytes<Retained>,
}

#[test]
fn generated_owned_and_borrowed_decoders_share_the_generic_parser() {
    let raw = br#"{"text":"a\"b","number":42}"#;
    let owned = General::decode_json(Shared::from_static(raw)).expect("owned decode");
    assert_eq!(owned.text.as_slice(), b"a\"b");
    assert_eq!(owned.number.as_slice(), b"42");

    let borrowed = General::decode_request(raw).expect("borrowed decode");
    assert_eq!(borrowed.text.as_slice(), b"a\"b");
    assert_eq!(borrowed.number.as_slice(), b"42");

    let plain_raw = br#"{"text":"alpha"}"#;
    let plain = Plain::decode_request(plain_raw).expect("plain borrowed decode");
    assert_eq!(plain.text.as_slice(), b"alpha");
}
