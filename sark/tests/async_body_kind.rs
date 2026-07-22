use http::StatusCode;
use o3::buffer::{Borrowed, Bytes};
use sark::service::{RouteSpec, manifold::NativeResponse};
use sark_core::http::body_kind::ResponseKind;

#[sark_gen::request]
struct Request {}

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: &'static [u8],
}

#[sark_gen::response(raw)]
struct BorrowedStaticReply<'req> {
    status: StatusCode,
    body: &'static [u8],
    #[header("x-value")]
    value: Bytes<Borrowed<'req>>,
}

#[sark_gen::response(raw)]
struct OwnedReply {
    status: StatusCode,
    body: Vec<u8>,
}

#[sark_gen::handler]
async fn reply(_request: Request, _state: &()) -> Reply {
    Reply {
        status: StatusCode::OK,
        body: b"ok",
    }
}

#[test]
fn async_body_kind_follows_response_type() {
    assert!(matches!(
        <reply as RouteSpec>::RESPONSE_BODY_KIND,
        ResponseKind::Static
    ));
}

#[test]
fn owned_generated_responses_keep_their_concrete_shape() {
    let static_shape: sark_core::http::StaticResponseInner<'_, 0> =
        NativeResponse::into_route_response(Reply {
            status: StatusCode::OK,
            body: b"ok",
        });
    let mut out = [0u8; 256];
    assert!(
        static_shape
            .write_head_only(&mut out, b"Thu, 01 Jan 1970 00:00:00 GMT")
            .is_some()
    );

    let _: sark_core::http::FixedResponseInner<'_, 0> =
        NativeResponse::into_route_response(OwnedReply {
            status: StatusCode::OK,
            body: b"ok".to_vec(),
        });
}

#[test]
fn borrowed_static_response_uses_a_static_shape() {
    type Response<'req> = <BorrowedStaticReply<'req> as NativeResponse<'req>>::Shape;

    assert!(matches!(
        <BorrowedStaticReply<'_> as NativeResponse<'_>>::BODY_KIND,
        ResponseKind::Static
    ));
    let response = BorrowedStaticReply {
        status: StatusCode::OK,
        body: b"ok",
        value: Bytes::<Borrowed<'_>>::from(b"v"),
    };
    let shape: sark_core::http::StaticResponseInner<'_, 1> =
        NativeResponse::into_route_response(response);
    let shape: Response<'_> = shape;
    let mut out = [0u8; 256];
    let (_, body) = shape
        .write_head_only(&mut out, b"Thu, 01 Jan 1970 00:00:00 GMT")
        .expect("borrowed static response head fits");
    assert_eq!(body, b"ok");
}
