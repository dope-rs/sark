pub mod common;
use common::response::{Counted, Fields, SliceSink, StaticReply, View};
use sark::http::{
    field::Value,
    payload,
    payload::{
        fixed,
        response::{dispatch::Complete, status::Code},
    },
};

#[test]
fn fixed_shape_stays_exact() {
    assert!(matches!(
        <StaticReply<'static> as Complete<'static, 'static>>::BODY,
        payload::Kind::Static
    ));
    let mut bytes = [0; 2];
    let (status, len) = StaticReply {
        status: Code::OK,
        body: fixed::Body::new(b"ok"),
    }
    .complete(SliceSink(&mut bytes, None))
    .expect("static reply");
    assert_eq!((status, &bytes[..len]), (Code::OK, b"ok".as_slice()));
}

#[test]
fn view_shape_is_static() {
    assert!(matches!(
        <View<'_, '_> as Complete<'_, '_>>::BODY,
        payload::Kind::Static
    ));
    let value = *b"v";
    let expected = b"x-value: v\r\n";
    let mut wire = Vec::with_capacity(expected.len());
    let mut fields = Fields {
        out: o3::buffer::write::Staged::try_new(&mut wire, expected.len()).expect("fields"),
        result: Ok(()),
    };
    let mut bytes = [0; 2];
    let (status, len) = View {
        status: Code::OK,
        body: fixed::Body::new(b"ok"),
        value: Value::try_bytes(&value).expect("borrowed field"),
    }
    .complete(SliceSink(&mut bytes, Some(&mut fields)))
    .expect("view reply");
    assert_eq!((status, &bytes[..len]), (Code::OK, b"ok".as_slice()));
    fields.out.try_complete().expect("exact fields").commit();
    assert_eq!(wire, expected);
}

#[test]
fn bounded_skips_exact_size() {
    let counts = Counted {
        bytes: b"true",
        ..Counted::default()
    };
    let response = sark_json::writing::Body::new(&counts)
        .try_response()
        .map_err(|rejected| rejected.discard())
        .expect("upper bound");
    let mut bytes = [0; 4];
    let (status, len) = response
        .complete(SliceSink(&mut bytes, None))
        .expect("bounded body");
    assert_eq!((status, &bytes[..len]), (Code::OK, b"true".as_slice()));
    assert_eq!(
        (counts.sizes.get(), counts.bounds.get(), counts.writes.get()),
        (0, 1, 1)
    );
}
