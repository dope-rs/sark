use std::convert;

use sark_core::decode::HeadError;
use sark_http::payload::response::status::Code;

#[test]
fn maps_head_statuses() {
    assert_eq!(
        HeadError::<convert::Infallible>::BadRequest.status(),
        Code::BAD_REQUEST,
    );
    assert_eq!(
        HeadError::<convert::Infallible>::ContentLength(sark_http::field::text::IntError::Empty)
            .status(),
        Code::BAD_REQUEST,
    );
    assert_eq!(
        HeadError::<convert::Infallible>::NotFound.status(),
        Code::NOT_FOUND,
    );
    assert_eq!(
        HeadError::<convert::Infallible>::Method(sark_http::method::Error::Syntax(
            sark_http::field::Error::Empty,
        ))
        .status(),
        Code::BAD_REQUEST,
    );
    assert_eq!(
        HeadError::<convert::Infallible>::Method(sark_http::method::Error::Allocation(
            o3::alloc::Exhausted,
        ))
        .status(),
        Code::INTERNAL_SERVER_ERROR,
    );

    let error = HeadError::Schema(7_u8);
    assert_eq!(error.status(), Code::BAD_REQUEST);
    assert!(matches!(error, HeadError::Schema(7)));
}
