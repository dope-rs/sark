use dope_test::alloc::Tracker;
use o3::{alloc, buffer::write::Sink};
use sark_http::field::metadata::{
    block::{Validation, Valued, buffer::Values},
    map::record::Span,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn rollback_restores_proof() {
    let mut buffer = Values::new();
    buffer
        .writer(None)
        .write_slice(b"text")
        .expect("value prefix");
    let mut writer = buffer.writer(None);
    writer
        .write_slice(b"\r\n")
        .expect("retained malformed bytes");
    writer.rollback();
    assert_eq!(buffer.as_slice(), b"text");
    assert_eq!(buffer.valued().validation(), Validation::Valid);
    buffer
        .writer(None)
        .write_slice(b"\n")
        .expect("retained malformed bytes");
    assert_eq!(buffer.valued().validation(), Validation::Malformed);
}

#[test]
fn final_fragment_validates() {
    let mut buffer = Values::new();
    buffer
        .writer(None)
        .write_slice(b"a ")
        .expect("value prefix");
    assert_eq!(buffer.valued().validation(), Validation::Malformed);
    buffer.writer(None).write_slice(b"b").expect("value suffix");
    assert_eq!(buffer.valued().validation(), Validation::Valid);
}

#[test]
fn append_failure() {
    let mut buffer = Values::new();
    buffer
        .copy_from(Valued::from_span(Span::new(b"\r", b"x")))
        .expect("cached malformed value");
    buffer.writer(None).write_slices([]).expect("empty append");
    assert_eq!(buffer.valued().validation(), Validation::Malformed);
    {
        let mut writer = buffer.writer(None);
        assert_eq!(
            Tracker::fail_after(0, || writer.write_slices([b"long-", b"value"])),
            Err(alloc::Error::Exhausted)
        );
        assert!(writer.is_retained());
    }
    assert_eq!(buffer.as_slice(), b"\rx");
    assert_eq!(buffer.valued().validation(), Validation::Malformed);
    buffer
        .writer(None)
        .write_slice(b"y")
        .expect("resume append");
    assert_eq!(buffer.as_slice(), b"\rxy");
    assert_eq!(buffer.valued().validation(), Validation::Malformed);
    assert_eq!(
        Tracker::fail_after(0, || {
            buffer.copy_from(Valued::from_span(Span::new(b"long-", b"value")))
        }),
        Err(alloc::Error::Exhausted)
    );
    assert!(buffer.is_empty());
    assert_eq!(buffer.valued().validation(), Validation::Valid);
}
