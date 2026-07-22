use std::cell::Cell;

#[test]
fn body_template_preserves_bytes_and_evaluates_each_hole_once() {
    let calls = Cell::new(0usize);
    let body = sark_gen::body!(
        "prefix {{{}}} ☃ {}",
        {
            calls.set(calls.get() + 1);
            Vec::from(&b"first"[..])
        },
        {
            calls.set(calls.get() + 1);
            Vec::from(&b"second"[..])
        },
    );

    assert_eq!(calls.get(), 2);
    assert_eq!(body, "prefix {first} ☃ second".as_bytes());
}

#[test]
fn body_template_without_holes_is_exact() {
    assert_eq!(sark_gen::body!("plain {{body}}"), b"plain {body}");
}
