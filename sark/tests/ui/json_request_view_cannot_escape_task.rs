use std::cell::Cell;

use http::StatusCode;
use o3::buffer::{Borrowed, Bytes, Retained};

struct State<'a> {
    leaked: Cell<Option<Bytes<Borrowed<'a>>>>,
}

#[sark_gen::json(ordered, plain)]
struct Body {
    payload: Bytes<Retained>,
}

#[sark_gen::request]
#[json_body(Body)]
struct Request {}

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: Vec<u8>,
}

#[sark_gen::handler]
async fn leak(request: Request, state: &State<'_>) -> Reply {
    state.leaked.set(Some(request.body.payload));
    Reply {
        status: StatusCode::OK,
        body: Vec::new(),
    }
}

fn main() {}
