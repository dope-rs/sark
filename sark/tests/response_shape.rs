pub mod common;
use common::response::{StaticReply, View};
use sark::{
    core::manifold::Response,
    http::{
        field::{self, Value},
        payload,
        payload::{
            fixed,
            response::{dispatch, dispatch::IntoOutput as _, status::Code},
        },
    },
};

#[test]
fn fixed_shape_stays_exact() {
    assert!(matches!(
        <StaticReply<'static> as Response<'static, 'static>>::BODY_KIND,
        payload::Kind::Static
    ));
    let shape = StaticReply {
        status: Code::OK,
        body: fixed::Body::new(b"ok"),
    }
    .into_output();
    let copy = shape;
    assert_eq!(shape.status(), Code::OK);
    assert_eq!(copy.body().as_slice(), b"ok");
}

#[test]
fn view_shape_is_static() {
    type Output<'req> = <View<'req, 'req> as dispatch::IntoOutput<'req, 'req>>::Output;

    assert!(matches!(
        <View<'_, '_> as Response<'_, '_>>::BODY_KIND,
        payload::Kind::Static
    ));
    let response = View {
        status: Code::OK,
        body: fixed::Body::new(b"ok"),
        value: field::literal!(Value::try_bytes(b"v")),
    };
    let shape: Output<'_> = response.into_output();
    let copy = shape;
    assert_eq!(shape.status(), Code::OK);
    assert_eq!(copy.body().as_slice(), b"ok");
}
