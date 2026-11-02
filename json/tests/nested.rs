use std::{convert::Infallible, num::NonZero};

use dope_core::{
    BranchError,
    driver::{Binding, life::recovery, schedule::ready::task::owner::Capacity, settings},
};
use dope_test::{
    alloc::Tracker,
    scope::{self, Driver},
};
use sark_http::request::Seq;
pub mod common;
use std::str;

use o3::buffer::storage::Shared;
use sark_json::{
    parsing::{Decode, Request},
    segment,
    writing::TryEncode,
};

use crate::common::nested::*;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn recursive_depth_decodes() {
    let json = Node::json(NonZero::new(16).expect("recursive depth"));
    let decoded = <Node as Decode>::decode_ref(json.as_bytes()).expect("shallow recursive decodes");
    let mut node = &decoded;
    let mut levels = 1;
    while let Some(child) = node.children.first() {
        node = child;
        levels += 1;
    }
    assert_eq!(levels, 16);
}

#[test]
fn recursive_limit_errors() {
    let json = Node::json(NonZero::new(5_000).expect("recursive depth"));
    let result = <Node as Decode>::decode_ref(json.as_bytes());
    assert!(
        result.is_err(),
        "deeply nested typed JSON must yield a decode error, not a stack overflow"
    );
}

#[test]
fn empty_object_round_trips() {
    assert_eq!(Empty {}.try_encode().expect("encode"), b"{}");
    <Empty as Decode>::decode_ref(b"{}").expect("empty object decodes");
}

#[test]
fn nested_array_encodes() {
    let response = Catalog::try_new(2).expect("nested catalog fixture");
    let bytes = response.try_encode().expect("encode");
    let wire = br#"{"id":1,"name":"Alpha","category":"electronics","stock":{"price":328,"quantity":15,"active":true},"tags":["fast","new"],"rating":{"score":48,"count":127},"total":14760}"#;
    let mut expected = Vec::new();
    expected.extend_from_slice(b"{\"items\":[");
    expected.extend_from_slice(wire);
    expected.extend_from_slice(b",");
    expected.extend_from_slice(wire);
    expected.extend_from_slice(b"],\"count\":2}");
    assert_eq!(bytes, expected);
}

#[test]
fn empty_arrays_encode() {
    let mut item = Item::try_new().expect("nested item fixture");
    item.tags = Vec::new();
    let bytes = item.try_encode().expect("encode");
    assert!(
        str::from_utf8(&bytes)
            .expect("JSON encoding is valid UTF-8")
            .contains(r#""tags":[],"#),
        "empty string array must encode as []"
    );

    let response = Catalog {
        items: Vec::new(),
        count: 0,
    };
    assert_eq!(
        response.try_encode().expect("encode"),
        br#"{"items":[],"count":0}"#
    );
}

#[test]
fn singleton_array_encodes() {
    let mut item = Item::try_new().expect("nested item fixture");
    item.tags = vec![sark_json::Text::try_copy("solo").expect("tag fixture")];
    let bytes = item.try_encode().expect("encode");
    assert!(
        str::from_utf8(&bytes)
            .expect("JSON encoding is valid UTF-8")
            .contains(r#""tags":["solo"],"#)
    );
}

#[test]
fn array_elements_escape() {
    let mut item = Item::try_new().expect("nested item fixture");
    item.tags = vec![
        sark_json::Text::try_copy("a\"b").expect("quote tag fixture"),
        sark_json::Text::try_copy("c\nd").expect("newline tag fixture"),
    ];
    let bytes = item.try_encode().expect("encode");
    assert!(
        str::from_utf8(&bytes)
            .expect("JSON encoding is valid UTF-8")
            .contains(r#""tags":["a\"b","c\nd"],"#)
    );
}

#[test]
fn tfb_message_shape() {
    let value = Message {
        message: sark_json::Text::try_copy("Hello, World!").expect("owned message fixture"),
    };
    assert_eq!(
        value.try_encode().expect("encode"),
        br#"{"message":"Hello, World!"}"#
    );
}

#[test]
fn world_shape() {
    let value = World {
        id: 4242,
        random_number: 88,
    };
    assert_eq!(
        value.try_encode().expect("encode"),
        br#"{"id":4242,"randomNumber":88}"#
    );

    let decoded =
        <World as Decode>::decode_ref(br#"{"id":4242,"randomNumber":88}"#).expect("world decodes");
    assert_eq!(decoded.id, 4242);
    assert_eq!(decoded.random_number, 88);
}

#[test]
fn fortune_escapes() {
    let value = Fortune {
        id: 11,
        message: sark_json::Text::try_copy("<script>alert(\"x\")</script>\n\tend")
            .expect("owned fortune fixture"),
    };
    let bytes = value.try_encode().expect("encode");
    assert_eq!(
        bytes,
        &br#"{"id":11,"message":"<script>alert(\"x\")</script>\n\tend"}"#[..]
    );
}

#[test]
fn control_escape_width() {
    let value = Fortune {
        id: 0,
        message: sark_json::Text::try_copy("\0a\u{1f}b").expect("owned control fixture"),
    };
    let bytes = value.try_encode().expect("encode");
    let mut expected = Vec::new();
    expected.extend_from_slice(br#"{"id":0,"message":""#);
    expected.extend_from_slice(b"\\u0000a\\u001fb");
    expected.extend_from_slice(br#""}"#);
    assert_eq!(bytes, expected);
}

#[test]
fn round_trip_decodes() {
    let response = Catalog::try_new(2).expect("nested catalog fixture");
    let mut encoded = response.try_encode().expect("encode");
    let encoded = Shared::try_from(&mut encoded).expect("encoded JSON storage remains shareable");
    let decoded = <Catalog as Decode>::decode(encoded).expect("owned catalog decodes");
    assert_eq!(decoded.count, 2);
    assert_eq!(decoded.items.len(), 2);
    let item = &decoded.items[0];
    assert_eq!(item.id, 1);
    assert_eq!(item.name.as_slice(), b"Alpha");
    assert_eq!(item.category.as_slice(), b"electronics");
    assert_eq!(item.stock.price, 328);
    assert!(item.stock.active);
    assert_eq!(item.tags.len(), 2);
    assert_eq!(item.tags[0].as_slice(), b"fast");
    assert_eq!(item.tags[1].as_slice(), b"new");
    assert_eq!(item.rating.score, 48);
    assert_eq!(item.rating.count, 127);
    assert_eq!(item.total, 14760);
}

#[test]
fn segmented_nested_decodes() {
    let response = Catalog::try_new(2).expect("nested catalog fixture");
    let encoded = response.try_encode().expect("encode");
    let first = encoded.len() / 3;
    let second = encoded.len() * 2 / 3;
    let mut decoder = segment::Decoder::<Catalog>::new(vec![
        Shared::try_copy(&encoded[..first]).expect("first segment"),
        Shared::try_copy(&encoded[first..second]).expect("middle segment"),
        Shared::try_copy(&encoded[second..]).expect("last segment"),
    ])
    .expect("decoder input");
    let mut turns = 0;
    let decoded = loop {
        turns += 1;
        let progress = decoder.poll(NonZero::<usize>::MIN.saturating_add(10));
        assert!(!matches!(progress, segment::Progress::NeedInput));
        match progress {
            segment::Progress::Ready(decoded) => break Ok(decoded),
            segment::Progress::Pending | segment::Progress::NeedInput => {}
            segment::Progress::Failed(error) => break Err(error),
        }
    }
    .expect("segmented nested decode must succeed");
    assert!(turns > 1);
    assert_eq!(decoded.count, 2);
    assert_eq!(decoded.items.len(), 2);
    assert_eq!(decoded.items[0].name.as_slice(), b"Alpha");
    assert_eq!(decoded.items[1].tags[1].as_slice(), b"new");
}

#[test]
fn request_fields_share_id() {
    let encoded = Catalog::try_new(2)
        .expect("nested catalog fixture")
        .try_encode()
        .expect("encoded catalog");
    let mut reports = Vec::new();
    Driver::new(settings::Config::default()).with_driver(
        |driver| {
            let binding = Binding::new(
                driver.driver_ref().timer(),
                Capacity::new(NonZero::<u32>::MIN),
            );
            let ids = Seq::new(binding.sequence());
            let id = ids.try_next().expect("request id");
            let owner = Shared::try_copy(&encoded).expect("input owner");
            let decoder =
                segment::Decoder::<CatalogScoped<'_>, _>::try_feed(id).expect("request decoder");
            let mut decoder = match decoder.push(owner.into()) {
                segment::Transfer::Accepted(decoder) => Some(decoder.close()),
                segment::Transfer::Rejected { .. } => None,
            }
            .expect("input owner");
            let request = loop {
                let (progress, activity) =
                    Tracker::activity(|| decoder.poll(NonZero::<usize>::MIN));
                match progress {
                    segment::Progress::Ready(request) => {
                        assert_eq!(activity.allocations, 0);
                        break Some(request);
                    }
                    segment::Progress::Pending => {}
                    _ => break None,
                }
            }
            .expect("scoped decode");
            let mut items = request.items.into_iter();
            let first = items.next().expect("first item");
            assert!(first.name.into_inner().id() == id);
            assert!(first.category.into_inner().id() == id);
            assert!(
                first
                    .tags
                    .into_iter()
                    .next()
                    .expect("first tag")
                    .into_inner()
                    .id()
                    == id
            );
            let second = items.next().expect("second item");
            assert!(
                second
                    .tags
                    .into_iter()
                    .nth(1)
                    .expect("second tag")
                    .into_inner()
                    .id()
                    == id
            );
        },
        recovery::Policy::new(
            |event| reports.push(event),
            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("native recovery: {:?}", failure.cause())
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(|error| match error {
                    scope::Error::Create(error) => BranchError::First(error.kind()),
                    scope::Error::Close(error) => BranchError::Second(error.cause()),
                })
                .expect("driver lifetime");
        },
    );
    assert!(reports.is_empty());
}

#[test]
fn nested_budget_borrows() {
    let response = Catalog::try_new(32).expect("nested catalog fixture");
    let encoded = response.try_encode().expect("encode");
    let needle = b"Alpha";
    let start = encoded
        .windows(needle.len())
        .position(|window| window == needle)
        .expect("encoded item name");
    let before = Shared::try_copy(&encoded[..start - 1]).expect("before segment");
    let selected =
        Shared::try_copy(&encoded[start - 1..start + needle.len() + 1]).expect("selected segment");
    let after = Shared::try_copy(&encoded[start + needle.len() + 1..]).expect("after segment");
    let expected = selected.as_slice()[1..].as_ptr();
    let wire_len = encoded.len();

    let mut decoder =
        segment::Decoder::<Catalog>::new(vec![before, selected, after]).expect("decoder input");
    let mut turns = 0usize;
    let mut decoded = loop {
        turns += 1;
        let progress = decoder.poll(NonZero::<usize>::MIN);
        assert!(!matches!(progress, segment::Progress::NeedInput));
        match progress {
            segment::Progress::Ready(decoded) => break Ok(decoded),
            segment::Progress::Pending | segment::Progress::NeedInput => {}
            segment::Progress::Failed(error) => break Err(error),
        }
    }
    .expect("segmented nested decode must succeed");

    assert!(
        turns > wire_len * 2,
        "nested scans must remain cooperative after the outer scan"
    );
    assert_eq!(decoded.items.len(), 32);
    assert_eq!(decoded.items[0].name.as_slice().as_ptr(), expected);

    assert!(<Catalog as segment::Decode>::release(&mut decoded, &mut 0).is_pending());
    assert_eq!(decoded.items[31].tags.len(), 2);
    for remaining in [1, 0] {
        assert!(<Catalog as segment::Decode>::release(&mut decoded, &mut 1).is_pending());
        assert_eq!(decoded.items.len(), 32);
        assert_eq!(decoded.items[31].tags.len(), remaining);
    }
    assert!(<Catalog as segment::Decode>::release(&mut decoded, &mut 1).is_pending());
    assert_eq!(decoded.items.len(), 31);
}

#[test]
fn request_view_borrows() {
    let request = Catalog::try_new(2).expect("nested catalog fixture");
    let encoded = request.try_encode().expect("encode");
    let view = <Catalog as Request>::decode(&encoded).expect("nested request view");
    let input = encoded.as_ptr() as usize..encoded.as_ptr() as usize + encoded.len();

    assert_eq!(view.items.len(), 2);
    assert_eq!(view.items[0].name.as_slice(), b"Alpha");
    assert_eq!(view.items[1].tags[1].as_slice(), b"new");
    assert!(input.contains(&(view.items[0].name.as_slice().as_ptr() as usize)));
    assert!(input.contains(&(view.items[1].tags[1].as_slice().as_ptr() as usize)));
}
