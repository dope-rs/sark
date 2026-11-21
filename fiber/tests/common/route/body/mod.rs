pub use chunks::{Chunks, Trace};
use sark_http::payload::response::status;

#[sark_fiber::response(raw)]
pub struct OwnedReply {
    pub status: status::Code,
    pub body: Vec<u8>,
}

#[sark_fiber::handler(pub)]
fn ping() -> OwnedReply {
    OwnedReply {
        status: status::Code::OK,
        body: b"pong".to_vec(),
    }
}

mod chunks;
pub mod discard;
pub mod stream;

pub mod length;

pub mod retained;

pub mod tunnels;
