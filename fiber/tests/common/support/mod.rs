use sark_http::payload::response::status;

use crate::common::{client::tls::Keys, server};

const SEED: [u8; 32] = [7; 32];
pub const BIG_LEN: usize = 512 * 1024;

#[sark_fiber::response(raw)]
pub struct Reply {
    status: status::Code,
    body: Vec<u8>,
}

#[sark_fiber::handler(pub)]
pub(crate) fn hello() -> Reply {
    Reply {
        status: status::Code::OK,
        body: b"hello".to_vec(),
    }
}

#[sark_fiber::handler(pub)]
pub(crate) fn big() -> Reply {
    let mut body = Vec::new();
    body.resize(BIG_LEN, b'x');
    Reply {
        status: status::Code::OK,
        body,
    }
}

sark_fiber::define_route! {
    pub Routes<'state>: server::State<'state> => {
        GET "/hello" => hello,
        POST "/hello" => hello,
        GET "/big" => big,
    }
}

#[derive(Clone, Copy)]
pub enum Mode {
    Plain,
    Tls,
}

pub fn tls() -> Keys {
    Keys::new(&SEED)
}

#[derive(Clone, Copy)]
pub enum Entry {
    Configured,
    Bound,
}

pub mod resource;
