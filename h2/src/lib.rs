use o3::buffer::hold;
use sark_core::manifold::stream::Section;
use sark_http::field::metadata::section::rules;

pub mod client;
pub mod conn;
pub mod flow;
pub mod frame;
pub mod hpack;
pub mod server;
pub mod stream;

/// Canonical H2 ingress owner policy.
/// Complete receive chunks share their owner unless its backing would retain
/// more than twice the logical bytes.
pub const INGRESS_RETENTION: hold::Policy = hold::Policy::new(1, 2);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Endpoint {
    Client,
    Server,
}

impl Endpoint {
    pub(crate) const fn section(self, section: Section) -> rules::Kind {
        match (self, section) {
            (Self::Client, Section::Initial) => rules::Kind::Response,
            (Self::Client, Section::Trailing) => rules::Kind::ResponseTrailers,
            (Self::Server, Section::Initial) => rules::Kind::Request,
            (Self::Server, Section::Trailing) => rules::Kind::RequestTrailers,
        }
    }
}

pub trait Role: Copy + Default {
    const ENDPOINT: Endpoint;
}

#[derive(Copy, Clone, Default)]
pub struct ServerRole;

impl Role for ServerRole {
    const ENDPOINT: Endpoint = Endpoint::Server;
}

#[derive(Copy, Clone, Default)]
pub struct ClientRole;

impl Role for ClientRole {
    const ENDPOINT: Endpoint = Endpoint::Client;
}

const _: () = {
    assert!(core::mem::size_of::<Endpoint>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Endpoint>() == core::mem::align_of::<bool>());
};
