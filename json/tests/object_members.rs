pub mod common;

use common::members::{
    Envelope, Generic, ITEM_VIEW, Item, ItemView, OWNED_ITEM, Offer, OwnedItem, Price, Rating,
};
use dope_test::alloc::Tracker;
use sark_json::parsing::Decode as _;
use sark_json::writing::{Members as _, TryEncode as _, TrySize as _, Writer};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn generic_members() {
    let value = Generic::<8> {
        price: Price {
            price: 3,
            quantity: 4,
        },
        token: sark_json::Text::try_new(sark_json::Token::new()).expect("text"),
    };
    assert_eq!(
        value.try_encode().expect("encode"),
        br#"{"price":3,"quantity":4,"token":""}"#
    );
    let mut output = b"{\"id\":7".to_vec();
    assert!(
        value
            .try_members::<_, false>(Writer::new(&mut output))
            .is_continue()
    );
    assert_eq!(output, br#"{"id":7,"price":3,"quantity":4,"token":"""#);
}

#[test]
fn encodes_flat_object() {
    let name = String::from("A\"B");
    let category = String::from("goods");
    let item = Item::new(&name, &category);
    let expected = br#"{"id":7,"name":"A\"B","category":"goods","price":3,"quantity":4,"active":true,"tags":["x","y"],"rating":{"score":5,"count":6},"total":12}"#;
    let (wire, (allocs, bytes)) = Tracker::usage(|| item.try_encode().expect("encode"));
    assert_eq!(wire, expected);
    assert_eq!((allocs, bytes), (1, wire.len()));
    assert!(std::ptr::eq(item.name.as_ptr(), name.as_ptr()));
    assert!(std::ptr::eq(item.category.as_ptr(), category.as_ptr()));
}

#[test]
fn writes_borrowed_buffer() {
    let name = String::from("borrowed");
    let item = Item::new(&name, "goods");
    let expected = item.try_encode().expect("encode");
    let mut wire = Vec::with_capacity(3 + expected.len());
    wire.extend_from_slice(b"pre");
    let (_, (allocs, bytes)) = Tracker::usage(|| item.try_append(&mut wire).expect("encode"));
    assert_eq!((allocs, bytes), (0, 0));
    assert_eq!(&wire[..3], b"pre");
    assert_eq!(&wire[3..], expected);
    assert!(std::ptr::eq(item.name.as_ptr(), name.as_ptr()));
}

#[test]
fn flattens_borrowed_decodable_object() {
    let item = OwnedItem::decode_ref(OWNED_ITEM).expect("item");
    let value = ItemView {
        item: &item,
        total: 12,
    };
    assert_eq!(value.try_size().expect("size"), ITEM_VIEW.len());
    let mut wire = Vec::with_capacity(ITEM_VIEW.len());
    let (_, usage) = Tracker::usage(|| value.try_append(&mut wire).expect("encode"));
    assert_eq!(wire, ITEM_VIEW);
    assert_eq!(usage, (0, 0));
}

#[test]
fn composes_first_and_last() {
    let name = String::from("offer");
    let envelope = Envelope {
        offer: Offer {
            price: Price {
                price: 3,
                quantity: 4,
            },
            name: sark_json::Text::from(name.as_str()),
            rating: Rating { score: 5, count: 6 },
        },
    };
    let expected = br#"{"price":3,"quantity":4,"name":"offer","score":5,"count":6}"#;
    let (wire, (allocs, bytes)) = Tracker::usage(|| envelope.try_encode().expect("encode"));
    assert_eq!(wire, expected);
    assert_eq!(
        envelope.try_fields().expect("size") + 2,
        envelope.try_size().expect("size")
    );
    assert_eq!((allocs, bytes), (1, expected.len()));

    let mut joined = b"{\"id\":7".to_vec();
    let start = joined.len();
    let writer = envelope
        .try_members::<_, false>(Writer::new(&mut joined))
        .continue_value()
        .expect("members");
    assert!(writer.put(b"}").is_continue());
    assert_eq!(
        joined.len() - start,
        envelope.try_fields().expect("size") + 2
    );
    assert_eq!(&joined[..8], b"{\"id\":7,");
    assert_eq!(&joined[8..], &expected[1..]);
}
