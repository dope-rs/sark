use std::mem::size_of;

use sark_core::http::{
    FixedResponseInner, HeaderItemInner, HeadersInner, MonoResponseInner, StaticResponseInner,
};

#[test]
fn exact_header_capacity_removes_unused_entries() {
    let item = size_of::<HeaderItemInner<'static>>();

    assert_eq!(
        size_of::<HeadersInner<'static, 4>>() - size_of::<HeadersInner<'static, 2>>(),
        2 * item
    );
    assert_eq!(
        size_of::<HeadersInner<'static, 2>>() - size_of::<HeadersInner<'static, 0>>(),
        2 * item
    );
    assert_eq!(
        size_of::<FixedResponseInner<'static, 4>>() - size_of::<FixedResponseInner<'static, 2>>(),
        2 * item
    );
    assert_eq!(
        size_of::<StaticResponseInner<'static, 4>>() - size_of::<StaticResponseInner<'static, 2>>(),
        2 * item
    );
}

#[test]
fn static_response_does_not_pay_for_hot_body_erasure() {
    assert!(
        size_of::<StaticResponseInner<'static, 2>>() < size_of::<MonoResponseInner<'static, 2>>()
    );
}

#[test]
fn item_array_defines_the_active_length() {
    let empty = HeadersInner::<'static, 0>::from_items([]);
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
}
