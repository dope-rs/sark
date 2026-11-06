use super::OwnedReply;
use sark_http::payload::response::status;

#[sark_fiber::handler(pub)]
fn ping() -> OwnedReply {
    let mut body = Vec::new();
    body.extend_from_slice(b"pong");
    OwnedReply {
        status: status::Code::OK,
        body,
    }
}

#[sark_fiber::handler(pub)]
fn echo() -> OwnedReply {
    OwnedReply {
        status: status::Code::OK,
        body: Vec::from(&b"echoed"[..]),
    }
}

sark_fiber::define_route! {
    pub ClOverflowDispatch: () => {
        GET "/ping" => ping,
        POST "/echo" => echo,
    }
}
