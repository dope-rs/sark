use sark_fiber::request::BodyLen;
use sark_http::payload::response::status;

use super::OwnedReply;

pub const PING: &[u8] = b"GET /ping HTTP/1.1\r\nHost: x\r\n\r\n";

#[sark_fiber::request]
pub struct UpReq {
    #[body_len]
    pub payload: BodyLen,
}

#[sark_fiber::handler(pub)]
#[max_body(16 * 1024 * 1024)]
fn up(req: UpReq) -> OwnedReply {
    let mut body = Vec::new();
    body.extend_from_slice(req.payload.len().to_string().as_bytes());
    OwnedReply {
        status: status::Code::OK,
        body,
    }
}

sark_fiber::define_route! {
    pub StreamDiscardDispatch: () => {
        GET "/ping" => crate::common::route::body::ping,
        POST "/up" => up,
    }
}

impl UpReq<'_> {
    pub fn head(total: usize) -> String {
        format!("POST /up HTTP/1.1\r\nHost: x\r\nContent-Length: {total}\r\n\r\n")
    }

    /// One counted upload whose header can be fragmented independently of its payload.
    pub fn wire(total: usize) -> Vec<u8> {
        let mut wire = Self::head(total).into_bytes();
        wire.resize(wire.len() + total, 0xa5);
        wire
    }
}
