use http::StatusCode;
use sark::service::RouteSpec;

#[sark_gen::request]
struct Request {}

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: &'static [u8],
}

#[sark_gen::handler]
async fn standard_macros(_request: Request, _state: &()) -> Reply {
    let values: Vec<usize> = vec![1, 2, 3];
    let formatted: String = format!("{}", 7);
    let matched: bool = matches!(values.as_slice(), [1, 2, 3]);
    let valid = matched && formatted == "7";
    Reply {
        status: if valid {
            StatusCode::OK
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        },
        body: if valid { b"ok" } else { b"invalid" },
    }
}

#[sark_gen::handler]
async fn early_return_preserves_nested_scopes(_request: Request, _state: &()) -> Reply {
    let nested = |value| {
        if value {
            return true;
        }
        false
    };
    if nested(true) {
        return Reply {
            status: StatusCode::OK,
            body: b"early",
        };
    }
    Reply {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        body: b"tail",
    }
}

#[test]
fn handler_canonicalizes_unqualified_standard_macros() {
    fn require_route<T: RouteSpec>() {}
    require_route::<standard_macros>();
    require_route::<early_return_preserves_nested_scopes>();
}
