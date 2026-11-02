use sark_json::{parsing, writing};
pub mod common;
use crate::common::decode::Number;

#[test]
fn direct_codegen() {
    let message = <Number as parsing::Decode>::decode_ref(br#"{"value":7}"#)
        .expect("the literal message must decode");
    assert_eq!(message.value, 7);
    assert_eq!(
        writing::TryEncode::try_encode(&message).expect("encode"),
        br#"{"value":7}"#
    );
}

#[test]
fn integer_leading_zero() {
    let invalid = br#"{"value":01}"#;
    assert!(matches!(
        <Number as parsing::Decode>::decode_ref(invalid),
        Err(parsing::Error::Parse(sark_json::Error::Invalid))
    ));
    assert!(matches!(
        <Number as parsing::Request>::decode(invalid),
        Err(parsing::Error::Parse(sark_json::Error::Invalid))
    ));
    let zero =
        <Number as parsing::Request>::decode(br#"{"value":0}"#).expect("zero integer must decode");
    assert_eq!(zero.value, 0);
}
