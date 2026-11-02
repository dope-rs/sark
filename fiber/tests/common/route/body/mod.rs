pub use chunks::Chunks;
use sark_http::payload::response::status;

#[sark_fiber::response(raw)]
pub struct OwnedReply {
    pub status: status::Code,
    pub body: Vec<u8>,
}

mod chunks;
pub mod discard;
pub mod stream;

pub mod length;
pub mod session;

pub mod retained;

pub mod tunnels;
