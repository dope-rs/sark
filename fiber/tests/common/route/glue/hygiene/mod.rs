use sark_http::payload::{fixed, response::status};

pub mod headers;

use headers::Reply;

#[sark_fiber::handler(pub)]
async fn use_std(request: headers::Request) -> Reply<'d> {
    let values: Vec<usize> = vec![1, 2, 3];
    let text: String = format!("{}", 7);
    let matched: bool = matches!(values.as_slice(), [1, 2, 3]);
    let valid = matched
        && text == "7"
        && request.__sark_m.as_ref() == b"a"
        && request.__sark_mode.as_ref() == b"abc"
        && request.eight.as_ref() == b"abcdefg";
    Reply {
        status: if valid {
            status::Code::OK
        } else {
            status::Code::INTERNAL_SERVER_ERROR
        },
        body: if valid {
            fixed::Body::new(b"ok")
        } else {
            fixed::Body::new(b"invalid")
        },
    }
}

#[sark_fiber::handler(pub)]
async fn keep_nested() -> Reply<'d> {
    let nested = |value| {
        if value {
            return true;
        }
        false
    };
    if nested(true) {
        return Reply {
            status: status::Code::OK,
            body: fixed::Body::new(b"early"),
        };
    }
    Reply {
        status: status::Code::INTERNAL_SERVER_ERROR,
        body: fixed::Body::new(b"tail"),
    }
}

sark_fiber::define_route! {
    pub HygieneRoutes: () => {
        GET "/standard" => request(header) async(capacity = 1, body = discarded) use_std,
        GET "/early" => async(capacity = 1, body = discarded) keep_nested,
    }
}
