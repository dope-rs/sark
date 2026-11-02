use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn,
    conn::{
        event::Role,
        io::{Receive, Send, Step},
        options,
    },
};
use sark_http::field::{literal, metadata::section::rules};

use crate::common::field::entry;

pub const UNCLASSIFIED: [&[u8]; 2] = [&[], &[0x40]];
pub const RESPONSES: [&[u8]; 3] = [b"103", b"100", b"200"];

#[derive(Debug)]
pub enum Error {
    Config(options::Error),
    Connection(conn::Error),
}

pub trait Stream {
    fn allow_push(&mut self, role: Role, maximum: u64) -> Result<(), Error>;
    fn promise(&mut self, id: u64) -> Result<(), conn::Error>;
}
impl<P: rules::Policy, A: rules::Push> Stream for conn::Connection<'_, P, A> {
    fn allow_push(&mut self, role: Role, maximum: u64) -> Result<(), Error> {
        match role {
            Role::Client => {
                self.control()
                    .open(Id::new(2).ok_or(Error::Connection(conn::Error::Identity { raw: 2 }))?)
                    .map_err(Error::Connection)?;
                let _ = self.events().poll_write().map_err(Error::Connection)?;
                Send::new(self)
                    .max_push(maximum)
                    .map_err(Error::Connection)?;
                let _ = self.events().poll_write().map_err(Error::Connection)?;
            }
            Role::Server => {
                let mut peer = conn::Connection::with_role(Role::Client).map_err(Error::Config)?;
                peer.control()
                    .open(Id::new(2).ok_or(Error::Connection(conn::Error::Identity { raw: 2 }))?)
                    .map_err(Error::Connection)?;
                Send::new(&mut peer)
                    .max_push(maximum)
                    .map_err(Error::Connection)?;
                if self.settings().peer().is_some() {
                    let _ = peer.events().poll_write().map_err(Error::Connection)?;
                }
                while let Some(write) = peer.events().poll_write().map_err(Error::Connection)? {
                    Receive::new(self)
                        .borrowed(write.stream_id(), &write.prefix, Boundary::More)
                        .map_err(Error::Connection)?;
                }
                while let Step::Item(_) | Step::More =
                    self.events().poll_token().map_err(Error::Connection)?
                {}
            }
        }
        Ok(())
    }
    fn promise(&mut self, id: u64) -> Result<(), conn::Error> {
        Send::new(self).promise(
            Id::ZERO,
            id,
            [
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":path", b"/asset")),
                literal!(entry(b":authority", b"example.test")),
            ],
        )?;
        Ok(())
    }
}
