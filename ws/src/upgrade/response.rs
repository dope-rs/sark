use sark_http::{
    field,
    h1::{codec::decode::fields, field::Connection},
    payload::response::status,
};

use crate::upgrade::{Error, Header, Need, Negotiation, state};

#[derive(Default)]
pub struct Admission {
    upgrade: state::State,
    accept: Option<[u8; 28]>,
}

impl<'input> fields::Sink<'input> for Admission {
    type Error = Error;

    fn push(&mut self, name: &'input [u8], value: &'input [u8]) -> Result<(), Self::Error> {
        if let Some(header) = Negotiation::from_name(name) {
            return Err(Error::Unsupported(header));
        }
        if name.eq_ignore_ascii_case(b"upgrade") {
            self.upgrade.push(value)?;
        } else if name.eq_ignore_ascii_case(b"sec-websocket-accept") {
            if self.accept.is_some() {
                return Err(Error::Duplicate(Header::Accept));
            }
            let value = field::Error::trim_ows(value);
            let Ok(accept) = <&[u8; 28]>::try_from(value) else {
                return Err(Error::AcceptLength {
                    actual: value.len(),
                });
            };
            self.accept = Some(*accept);
        }
        Ok(())
    }
}

impl Admission {
    pub fn finish(
        self,
        wire: u8,
        status: status::Code,
        conn: Connection,
    ) -> Result<[u8; 28], Error> {
        if wire != 1 {
            return Err(Error::Version { actual: wire });
        }
        if status != status::Code::SWITCHING_PROTOCOLS {
            return Err(Error::Status(status));
        }
        self.upgrade.require()?;
        if conn.is_close() {
            return Err(Error::Close);
        }
        if !conn.is_upgrade() {
            return Err(Error::Connection);
        }
        self.accept.ok_or(Error::Missing(Need::Accept))
    }
}

const _: () = assert!(core::mem::size_of::<Admission>() <= 32);
