use std::mem::size_of;

use sark_http::payload::response::{
    fixed,
    headers::{
        Name,
        head::{Head, Template},
        set::Set,
        value::Item,
    },
    statics,
};

#[test]
fn capacity_is_exact() {
    let item = size_of::<Item<'static>>();

    assert_eq!(size_of::<Name<&[u8]>>(), size_of::<&[u8]>());

    assert_eq!(
        size_of::<Set<'static, 4>>() - size_of::<Set<'static, 2>>(),
        2 * item
    );
    assert_eq!(
        size_of::<Set<'static, 2>>() - size_of::<Set<'static, 0>>(),
        2 * item
    );
    assert_eq!(
        size_of::<fixed::Direct<Head<'static, 4>, ()>>()
            - size_of::<fixed::Direct<Head<'static, 2>, ()>>(),
        2 * item
    );
    assert_eq!(
        size_of::<statics::Direct<'static, Head<'static, 4>>>()
            - size_of::<statics::Direct<'static, Head<'static, 2>>>(),
        2 * item
    );
}

#[test]
fn template_is_one_ref() {
    assert_eq!(
        size_of::<Head<'static, 0>>(),
        size_of::<&'static Template>() + size_of::<Set<'static, 0>>()
    );
}

#[test]
fn array_sets_len() {
    let empty = Set::<'static, 0>::from_items([]);
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
}
