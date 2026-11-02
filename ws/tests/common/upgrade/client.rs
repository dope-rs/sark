use sark_http::{h1::codec::decode::response, payload::response::Method};
use sark_ws::upgrade::{self, response::Admission};
pub const RESPONSE: &[u8] = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
pub const ACCEPT: &[u8; 28] = b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
pub const INVALID_FIELDS: [(&[u8], upgrade::FieldError); 5] = [
    (
        b" \th2c/1, \tbad name/v/v",
        upgrade::FieldError::Slash { offset: 20 },
    ),
    (
        b" \th2c/1, \tcustom/ \t",
        upgrade::FieldError::Version {
            offset: 17,
            byte: None,
        },
    ),
    (
        b" \th2c/1, \t/x",
        upgrade::FieldError::Protocol {
            offset: 10,
            byte: None,
        },
    ),
    (
        b" \th2c/1, \tcustom/v x",
        upgrade::FieldError::Version {
            offset: 18,
            byte: Some(b' '),
        },
    ),
    (b" ,\t, ", upgrade::FieldError::Missing { actual: 5 }),
];
pub const REJECTED_FIELD: &[u8] = b"websocket, bad//";
pub const RETAINED_FIELDS: [(Option<&[u8]>, upgrade::Error); 3] = [
    (None, upgrade::Error::Missing(upgrade::Need::Upgrade)),
    (Some(b"h2c"), upgrade::Error::Protocol),
    (Some(b"websocket"), upgrade::Error::Connection),
];

pub enum Error {
    Decode(response::Error<upgrade::Error>),
    Incomplete { actual: usize },
    Handshake(upgrade::Error),
}

pub trait Admit {
    fn try_admit(&self) -> Result<[u8; 28], Error>;
}

impl Admit for [u8] {
    fn try_admit(&self) -> Result<[u8; 28], Error> {
        let head = response::Decoder::new(Method::Other)
            .parse(self, usize::MAX, Admission::default())
            .map_err(Error::Decode)?
            .ok_or(Error::Incomplete { actual: self.len() })?;
        head.fields
            .finish(head.version, head.status, head.control.connection())
            .map_err(Error::Handshake)
    }
}
