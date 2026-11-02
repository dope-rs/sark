use core::array;
use std::{net::Ipv4Addr, sync::Arc};

use dope_tls::{config, rustls, tls::client::Plan};
use sark::http::payload::response::dispatch::{Stream, chunk::Chunk};
use sark_fiber::fiber::{IntoStream as _, IterStream};
use sark_http::payload::response::status;

use crate::common::server;

pub mod large;

pub const SEED: [u8; 32] = [11; 32];

pub const XML: &[u8] = b"<whoami><bookmaker_details/></whoami>";

#[sark_fiber::response(raw)]
pub struct XmlReply {
    pub status: status::Code,
    pub body: Vec<u8>,
}

#[sark_fiber::handler(pub)]
pub(crate) fn xml() -> XmlReply {
    XmlReply {
        status: status::Code::OK,
        body: XML.to_vec(),
    }
}

#[sark_fiber::handler(pub)]
pub(crate) fn chunk() -> Stream<'req, IterStream<array::IntoIter<Chunk<'d>, 1>>> {
    [Chunk::from(b"ok")].into_stream()
}

sark_fiber::define_route! {
    pub TlsDispatch<'state>: server::State<'state> => {
        GET "/xml" => xml,
        GET "/chunk" => stream(capacity = 16) chunk,
    }
}

pub struct Keys([u8; 32]);

impl Keys {
    pub fn new(seed: &[u8; 32]) -> Self {
        Self(*seed)
    }

    pub fn client_plan(&self) -> Result<Plan, dope_tls::Error> {
        Ok(Plan::new(self.client()?, Ipv4Addr::LOCALHOST.into()))
    }

    pub fn client(&self) -> Result<Arc<rustls::ClientConfig>, dope_tls::Error> {
        let key = config::raw::Identity::from_seed(&self.0)?.public_key();
        config::raw::Identity::client(key).map(Arc::new)
    }

    pub fn server(self) -> Result<Arc<rustls::ServerConfig>, dope_tls::Error> {
        config::raw::Identity::from_seed(&self.0)?
            .server()
            .map(Arc::new)
    }
}
