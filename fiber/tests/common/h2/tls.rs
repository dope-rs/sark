use std::{cell::Cell, io, io::Write, sync::Arc};

use dope_test::fibers::Yield;
use dope_tls::{config::raw::Identity, rustls};
use sark_h2::{ClientRole, conn::machine::Connection};
use sark_http::{
    field::{self, Entry, Pseudo, Value},
    payload::{fixed, response::status::Code},
};

use crate::common::client::tls::SEED;

pub const RESPONSE: &[u8] = b"shared async route";
pub const HEADERS: [Entry<'static>; 4] = [
    Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"GET"))),
    Entry::pseudo(Pseudo::Scheme, field::literal!(Value::try_bytes(b"https"))),
    Entry::pseudo(Pseudo::Path, field::literal!(Value::try_bytes(b"/shared"))),
    Entry::pseudo(
        Pseudo::Authority,
        field::literal!(Value::try_bytes(b"localhost")),
    ),
];

pub struct State(pub Cell<usize>);

#[sark_fiber::response(raw)]
pub struct Reply<'d> {
    status: Code,
    body: fixed::Body<'d>,
}

#[sark_fiber::handler(pub)]
async fn shared(state: &State) -> Reply<'d> {
    Yield::new().await;
    state.0.set(state.0.get() + 1);
    Reply {
        status: Code::OK,
        body: fixed::Body::new(RESPONSE),
    }
}

sark_fiber::define_route! {
    pub Shared: State => {
        GET "/shared" => async(capacity = 1, body = discarded) shared,
    }
}

pub fn server_config(
    fragment: Option<usize>,
) -> Result<Arc<rustls::ServerConfig>, dope_tls::Error> {
    let mut config = Identity::from_seed(&SEED)?.server()?;
    config.alpn_protocols.push(b"h2".to_vec());
    config.max_fragment_size = fragment;
    Ok(Arc::new(config))
}

pub fn client_config() -> Result<Arc<rustls::ClientConfig>, dope_tls::Error> {
    let key = Identity::from_seed(&SEED)?.public_key();
    let mut config = Identity::client(key)?;
    config.alpn_protocols.push(b"h2".to_vec());
    Ok(Arc::new(config))
}

pub fn flush(client: &mut Connection<ClientRole>, wire: &mut impl Write) -> io::Result<()> {
    loop {
        let output = client.output().first();
        if output.is_empty() {
            return wire.flush();
        }
        wire.write_all(output)?;
        let length = output.len();
        client.transfer().drain(length);
    }
}
