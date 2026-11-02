//! Generated bindings preserve user generic names.
//!
//! ```
//! # mod common;
//! use common::guard::*;
//! use std::{cell::Cell, ops::Deref};
//! use sark::http::payload::response::{dispatch::IntoOutput as _, status::Code};
//!
//! const value_: usize = 0;
//!
//! #[sark::response(json)]
//! pub struct Generic<const sink: usize = 0, const value: usize = value_, T: sark_json::writing::TryEncode = u64> {
//!     pub status: Code,
//!     pub body: T,
//! }
//!
//! sark::define_router! {
//!     pub Router<r#__V, const r#state: usize>: State<r#__V, r#state>
//!     where r#__V: Deref<Target = Cell<usize>>
//!     => {
//!         scope "" guard (EchoPath,) => [
//!             GET "/guard/:value" => request(path) unreachable,
//!         ],
//!     }
//! }
//!
//! fn main() {
//!     let calls = Cell::new(0);
//!     let state = State::<_, 1> { calls: &calls };
//!     Router::new(&state);
//!     let response: Generic = Generic {
//!         status: Code::OK,
//!         body: 42,
//!     };
//!     let output = response.into_output();
//!     assert_eq!(output.status(), Code::OK);
//! }
//! ```

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

#[test]
fn generic_names() -> Result<(), dope_test::checks::Error> {
    let suite = dope_test::checks::Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &[
            "dope",
            "dope-quic",
            "dope-test",
            "o3",
            "sark",
            "sark-client",
            "sark-core",
            "sark-gen",
            "sark-h1",
            "sark-h2",
            "sark-h3",
            "sark-http",
            "sark-json",
        ],
    );
    let exe = std::env::current_exe()?;
    let output = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.output()?;
    let suite = suite.try_collect(&exe, output)?;
    let status = suite.try_rustdoc()?.status()?;
    assert!(status.success(), "generic name contracts: {status}");
    Ok(())
}
