mod key;
pub mod request;
pub mod response;
mod state;

pub use key::Key;
use sark_http::{field::base64, h1::framing, payload::response::status};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldError {
    Missing { actual: usize },
    Slash { offset: usize },
    Protocol { offset: usize, byte: Option<u8> },
    Version { offset: usize, byte: Option<u8> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyError {
    Length { actual: usize },
    Base64(base64::Error),
    Decoded { actual: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Header {
    Version,
    Key,
    Accept,
    Origin,
    Cookie,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Negotiation {
    Extensions,
    Protocol,
}

impl Negotiation {
    pub(crate) fn from_name(name: &[u8]) -> Option<Self> {
        if name.eq_ignore_ascii_case(b"sec-websocket-extensions") {
            Some(Self::Extensions)
        } else if name.eq_ignore_ascii_case(b"sec-websocket-protocol") {
            Some(Self::Protocol)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Need {
    Upgrade,
    Version,
    Key,
    Accept,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Unsupported(Negotiation),
    Field(FieldError),
    Key(KeyError),
    Duplicate(Header),
    Missing(Need),
    Method {
        offset: usize,
        actual: Option<u8>,
        want: Option<u8>,
    },
    Target {
        offset: usize,
        actual: Option<u8>,
        want: Option<u8>,
    },
    Version {
        actual: u8,
    },
    WsVersion {
        offset: usize,
        actual: Option<u8>,
        want: Option<u8>,
    },
    Body(framing::RequestBody),
    Close,
    Connection,
    Protocol,
    Status(status::Code),
    AcceptLength {
        actual: usize,
    },
}

const _: () = {
    assert!(core::mem::size_of::<Error>() == core::mem::size_of::<base64::Error>());
    assert!(core::mem::size_of::<KeyError>() == core::mem::size_of::<base64::Error>());
    assert!(!core::mem::needs_drop::<Error>());
};
