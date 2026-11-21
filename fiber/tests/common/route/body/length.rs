use super::OwnedReply;
use sark_http::payload::response::status;

#[sark_fiber::handler(pub)]
fn echo() -> OwnedReply {
    OwnedReply {
        status: status::Code::OK,
        body: Vec::from(&b"echoed"[..]),
    }
}

sark_fiber::define_route! {
    pub ClOverflowDispatch: () => {
        GET "/ping" => crate::common::route::body::ping,
        POST "/echo" => echo,
    }
}
