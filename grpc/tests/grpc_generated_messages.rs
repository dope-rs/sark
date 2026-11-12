pub mod common;

use common::messages::{self, Borrowed, Packet};
use o3::{
    buffer::{segments, storage},
    mem::Rejected,
};
use sark_grpc::{
    Codec as _,
    details::Diagnostic,
    encode::{Message as _, Plan as _, Prost},
    message::Frame,
};

#[test]
fn borrowed_message_plan() {
    let data = b"borrowed protobuf payload".to_vec();
    let message = Borrowed { data: &data };
    let mut encoded = storage::Shared::new();
    message
        .plan()
        .write(&mut encoded)
        .expect("borrowed message encoding");
    let mut input = segments::Pool::try_new(segments::Plan {
        segments: 2,
        chains: 1,
    })
    .expect("two retained segments")
    .into_chain();
    let (first, second) = encoded.as_slice().split_at(encoded.as_slice().len() / 2);
    for bytes in [first, second] {
        input
            .try_push(storage::Shared::try_copy(bytes).expect("retained protobuf fragment"))
            .map_err(Rejected::discard)
            .expect("admitted segment capacity");
    }
    let frame = Frame::from_payload(input.into());
    let decoded = Prost::<Packet>::new()
        .decode_frame(frame)
        .expect("matching protobuf bytes across retained segments");
    assert_eq!(decoded.data, data);
}

#[test]
fn retains_decode_error() {
    let expected = <Packet as prost::Message>::decode(messages::TRUNCATED)
        .err()
        .expect("truncated byte field");
    let mut input = segments::Pool::try_new(segments::Plan {
        segments: 1,
        chains: 0,
    })
    .expect("one retained segment")
    .into_chain();
    input
        .try_push(storage::Shared::try_copy(messages::TRUNCATED).expect("retained byte field"))
        .map_err(Rejected::discard)
        .expect("admitted segment capacity");
    let frame = Frame::from_payload(input.into());
    let error = Prost::<Packet>::new()
        .decode_frame(frame)
        .err()
        .expect("truncated byte field");
    assert_eq!(error.code(), sark_grpc::Code::Internal);
    assert!(matches!(
        Diagnostic::from(&error),
        Diagnostic::Decode(actual) if actual == &expected
    ));
}
