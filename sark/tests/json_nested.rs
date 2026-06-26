use o3::buffer::Shared;
use sark::json::{JsonDecode, JsonEncode, Scratch};
use sark_core::http::LocalFrameBytes;

#[sark_gen::json(ordered)]
struct Message {
    message: LocalFrameBytes,
}

#[allow(non_snake_case)]
#[sark_gen::json(ordered)]
struct World {
    id: u64,
    randomNumber: u64,
}

#[sark_gen::json(ordered)]
struct Fortune {
    id: u64,
    message: LocalFrameBytes,
}

#[sark_gen::json(ordered)]
struct Rating {
    score: u64,
    count: u64,
}

#[sark_gen::json(ordered)]
struct Item {
    id: u64,
    name: LocalFrameBytes,
    category: LocalFrameBytes,
    price: u64,
    quantity: u64,
    active: bool,
    #[field(seq)]
    tags: Vec<LocalFrameBytes>,
    #[field(nested)]
    rating: Rating,
    total: u64,
}

#[sark_gen::json(ordered)]
struct ItemsResponse {
    #[field(seq, nested)]
    items: Vec<Item>,
    count: u64,
}

fn lfb(value: &'static [u8]) -> LocalFrameBytes {
    LocalFrameBytes::from_shared(Shared::from_static(value))
}

fn encode<T: JsonEncode>(value: &T) -> Vec<u8> {
    let out = value.encode_json();
    assert_eq!(
        value.json_len(),
        out.as_ref().len(),
        "json_len must equal encoded length"
    );
    out.as_ref().to_vec()
}

fn sample_item() -> Item {
    Item {
        id: 1,
        name: lfb(b"Alpha"),
        category: lfb(b"electronics"),
        price: 328,
        quantity: 15,
        active: true,
        tags: vec![lfb(b"fast"), lfb(b"new")],
        rating: Rating {
            score: 48,
            count: 127,
        },
        total: 14760,
    }
}

#[test]
fn nested_object_encodes() {
    let rating = Rating {
        score: 48,
        count: 127,
    };
    assert_eq!(encode(&rating), br#"{"score":48,"count":127}"#);
}

#[test]
fn item_with_array_and_nested_encodes() {
    let bytes = encode(&sample_item());
    assert_eq!(
        bytes,
        br#"{"id":1,"name":"Alpha","category":"electronics","price":328,"quantity":15,"active":true,"tags":["fast","new"],"rating":{"score":48,"count":127},"total":14760}"#
    );
}

#[test]
fn array_of_nested_objects_encodes() {
    let response = ItemsResponse {
        items: vec![sample_item(), sample_item()],
        count: 2,
    };
    let bytes = encode(&response);
    let expected_item = br#"{"id":1,"name":"Alpha","category":"electronics","price":328,"quantity":15,"active":true,"tags":["fast","new"],"rating":{"score":48,"count":127},"total":14760}"#;
    let mut expected = Vec::new();
    expected.extend_from_slice(b"{\"items\":[");
    expected.extend_from_slice(expected_item);
    expected.extend_from_slice(b",");
    expected.extend_from_slice(expected_item);
    expected.extend_from_slice(b"],\"count\":2}");
    assert_eq!(bytes, expected);
}

#[test]
fn empty_arrays_encode_as_brackets() {
    let mut item = sample_item();
    item.tags = Vec::new();
    let bytes = encode(&item);
    assert!(
        std::str::from_utf8(&bytes)
            .unwrap()
            .contains(r#""tags":[],"#),
        "empty string array must encode as []"
    );

    let response = ItemsResponse {
        items: Vec::new(),
        count: 0,
    };
    assert_eq!(encode(&response), br#"{"items":[],"count":0}"#);
}

#[test]
fn single_element_array_has_no_comma() {
    let mut item = sample_item();
    item.tags = vec![lfb(b"solo")];
    let bytes = encode(&item);
    assert!(
        std::str::from_utf8(&bytes)
            .unwrap()
            .contains(r#""tags":["solo"],"#)
    );
}

#[test]
fn array_elements_are_escaped() {
    let mut item = sample_item();
    item.tags = vec![lfb(b"a\"b"), lfb(b"c\nd")];
    let bytes = encode(&item);
    assert!(
        std::str::from_utf8(&bytes)
            .unwrap()
            .contains(r#""tags":["a\"b","c\nd"],"#)
    );
}

#[test]
fn tfb_message_shape() {
    let value = Message {
        message: lfb(b"Hello, World!"),
    };
    assert_eq!(encode(&value), br#"{"message":"Hello, World!"}"#);
}

#[test]
fn world_shape() {
    let value = World {
        id: 4242,
        randomNumber: 88,
    };
    assert_eq!(encode(&value), br#"{"id":4242,"randomNumber":88}"#);
}

#[test]
fn fortune_shape_with_escapes() {
    let value = Fortune {
        id: 11,
        message: lfb(b"<script>alert(\"x\")</script>\n\tend"),
    };
    let bytes = encode(&value);
    assert_eq!(
        bytes,
        &br#"{"id":11,"message":"<script>alert(\"x\")</script>\n\tend"}"#[..]
    );
}

#[test]
fn control_chars_six_byte_escape_identity() {
    let value = Fortune {
        id: 0,
        message: lfb(&[0x00, b'a', 0x1f, b'b']),
    };
    let bytes = encode(&value);
    let mut expected = Vec::new();
    expected.extend_from_slice(br#"{"id":0,"message":""#);
    expected.extend_from_slice(b"\\u0000a\\u001fb");
    expected.extend_from_slice(br#""}"#);
    assert_eq!(bytes, expected);
}

#[test]
fn scratch_reuse_yields_identical_bytes() {
    let scratch: Scratch<Item> = Scratch::new();
    let mut prev: Option<Vec<u8>> = None;
    for _ in 0..5 {
        let mut items = scratch.take();
        items.push(sample_item());
        items.push(sample_item());
        let response = ItemsResponse { items, count: 2 };
        let bytes = encode(&response);
        if let Some(ref expected) = prev {
            assert_eq!(
                &bytes, expected,
                "scratch reuse must produce identical bytes"
            );
        }
        scratch.give(response.items);
        prev = Some(bytes);
    }
}

#[test]
fn round_trips_through_decode() {
    let response = ItemsResponse {
        items: vec![sample_item(), sample_item()],
        count: 2,
    };
    let encoded = response.encode_json();
    let decoded = ItemsResponse::decode_json(encoded.freeze()).expect("decode");
    assert_eq!(decoded.count, 2);
    assert_eq!(decoded.items.len(), 2);
    let item = &decoded.items[0];
    assert_eq!(item.id, 1);
    assert_eq!(item.name.as_bytes(), b"Alpha");
    assert_eq!(item.category.as_bytes(), b"electronics");
    assert_eq!(item.price, 328);
    assert!(item.active);
    assert_eq!(item.tags.len(), 2);
    assert_eq!(item.tags[0].as_bytes(), b"fast");
    assert_eq!(item.tags[1].as_bytes(), b"new");
    assert_eq!(item.rating.score, 48);
    assert_eq!(item.rating.count, 127);
    assert_eq!(item.total, 14760);
}
