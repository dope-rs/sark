use sark_http::payload::response::status;

use crate::common::route::body::OwnedReply;

#[sark_fiber::handler(pub)]
fn gzip() -> OwnedReply {
    let mut body = Vec::new();
    body.extend_from_slice(b"hello");
    OwnedReply {
        status: status::Code::OK,
        body,
    }
}

sark_fiber::define_route! {
    pub HelloDispatch: () => {
        GET "/hello" => gzip,
    }
}
