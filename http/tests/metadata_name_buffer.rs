use std::mem;

use o3::buffer::write::Sink;
use sark_http::field::metadata::{
    block::{Name, Named, buffer::Names},
    map::record::Span,
};

#[test]
fn rollback_retains_owner() {
    let mut first = Names::new();
    let mut second = Names::new();
    first.writer(None).write_slice(b"ho").expect("name prefix");
    second.writer(None).write_slice(b"te").expect("name prefix");
    let mut left = first.writer(None);
    let mut right = second.writer(None);
    left.write_slice(b"st").expect("name suffix");
    right.write_slice(b"x").expect("name suffix");
    mem::swap(&mut left, &mut right);
    left.rollback();
    right.rollback();
    assert_eq!(first.as_slice(), b"ho");
    assert_eq!(second.named().proof(), Name::classify(b"te"));
    first.writer(None).write_slice(b"st").expect("name suffix");
    assert_eq!(first.named().proof(), Name::classify(b"host"));
}

#[test]
fn bounded_retains_proof() {
    let mut buffer = Names::new();
    buffer
        .copy_from(Named::from_span(Span::new(b"h", b"o")))
        .expect("cached name");
    let mut writer = buffer.writer(Some(2));
    writer
        .write_slice(b"x")
        .expect("count over the retention limit");
    assert!(!writer.is_retained());
    writer.rollback();
    assert_eq!(buffer.named().proof(), Name::classify(b"ho"));
    buffer
        .writer(None)
        .write_slices([b"s", b"t"])
        .expect("extend cached name");
    assert_eq!(buffer.as_slice(), b"host");
    assert_eq!(buffer.named().proof(), Name::classify(b"host"));
}
