use sark_http::payload::response::status;

#[sark_fiber::response(raw)]
pub struct PingReply {
    pub status: status::Code,
    pub body: Vec<u8>,
}

#[sark_fiber::handler(pub)]
fn ping() -> PingReply {
    let mut body = Vec::new();
    body.extend_from_slice(b"pong");
    PingReply {
        status: status::Code::OK,
        body,
    }
}

#[sark_fiber::response(raw)]
pub struct EchoReply {
    pub status: status::Code,
    pub body: Vec<u8>,
}

#[sark_fiber::handler(pub)]
fn echo() -> EchoReply {
    EchoReply {
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
