use std::mem::size_of;

use sark_core::http::{FixedResponse, HeaderItem, Headers, MonoResponseInner, StaticResponseInner};

#[test]
fn exact_header_capacity_removes_unused_entries() {
    let item = size_of::<HeaderItem<'static>>();

    assert_eq!(
        size_of::<Headers<'static, 4>>() - size_of::<Headers<'static, 2>>(),
        2 * item
    );
    assert_eq!(
        size_of::<Headers<'static, 2>>() - size_of::<Headers<'static, 0>>(),
        2 * item
    );
    assert_eq!(
        size_of::<FixedResponse<'static, 4>>() - size_of::<FixedResponse<'static, 2>>(),
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
    let empty = Headers::<'static, 0>::from_items([]);
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
}
