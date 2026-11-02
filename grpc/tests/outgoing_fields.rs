use sark_grpc::{
    metadata::{Error, Metadata},
    protocol::{Block, request},
};
use sark_http::field::metadata::map::record::Entry;

#[test]
fn admits_pseudo_values() {
    let metadata = Metadata::new();
    assert!(request::Fields::new(b"/svc/Method\r", None, &metadata).is_err());
    assert!(request::Fields::new(b"/svc/Method", Some(b"host\n"), &metadata).is_err());
}

#[test]
fn block_keeps_field_proofs() {
    let mut block = Block::new();
    block.push(b":status", b"200").expect("status");
    assert!(block.push(b":unknown", b"value").is_err());
    assert!(block.push(b"X-name", b"value").is_err());
    assert!(block.push(b"x-name", b"bad\rvalue").is_err());
    block.push(b"x-name", b"value").expect("field");
    assert!(block.iter().eq([
        Entry::new(b":status", b"200"),
        Entry::new(b"x-name", b"value"),
    ]));
}

#[test]
fn reserves_framing() {
    let mut metadata = Metadata::new();
    assert!(matches!(
        metadata.push(b"content-length", b"0"),
        Err(Error::ReservedName)
    ));
    assert!(metadata.is_empty());
}
