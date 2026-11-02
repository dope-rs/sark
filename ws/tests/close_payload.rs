use sark_ws::{
    fragment,
    inbound::{Close, Fault},
};

#[test]
fn retains_payload() {
    let bytes = b"\x03\xe8done";
    let close = Close::parse(bytes).expect("close");
    assert_eq!(close.payload().as_ptr(), bytes.as_ptr());
    assert_eq!(close.code(), Some(1000));
    assert_eq!(close.reason(), "done");
    assert!(Close::parse(&[]).expect("close").code().is_none());
}

#[test]
fn rejects_lengths() {
    assert_eq!(
        Close::parse(&[0; 126]).err(),
        Some(Fault::Control {
            fin: true,
            length: 126
        })
    );
    assert_eq!(
        Close::parse(&[0]).err(),
        Some(Fault::CloseLength {
            actual: 1,
            required: 2
        })
    );
}

#[test]
fn rejects_contents() {
    assert_eq!(
        Close::parse(&[0x03, 0xed]).err(),
        Some(Fault::CloseCode { actual: 1005 })
    );
    assert_eq!(
        Close::parse(&[0x03, 0xe8, 0xff]).err(),
        Some(Fault::CloseText(fragment::Utf8 {
            valid: 0,
            error: Some(1)
        }))
    );
}
