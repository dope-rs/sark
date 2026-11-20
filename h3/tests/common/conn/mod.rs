use sark_http::field::literal;
pub mod bridge;
pub mod build;
pub mod native;
pub mod payload;

use dope_quic::wire::stream::Id;
use o3::buffer::hold;
use sark_core::manifold::stream::Boundary;
use sark_h3::conn::{
    self,
    event::{Event, Role, Token},
    io::{Receive, Send, Step},
    options::{self, Config, Protocol},
};
use sark_http::field::metadata::{
    block::Connect,
    section::{rules, state::Selected},
};

use crate::common::{conn::payload::Write as _, field::entry, message::capacity::Capacity as _};

/// Exhaustive event adapter for deterministic tests.
pub fn next_event<'d, P: rules::Policy, A: rules::Push>(
    conn: &mut conn::Connection<'d, P, A>,
) -> Result<Option<Event<'d, Selected<P>, P::Block, Token>>, conn::Error> {
    loop {
        match conn.events().poll_token()? {
            Step::Item(event) => return Ok(Some(event)),
            Step::More => {}
            Step::Idle => return Ok(None),
        }
    }
}

pub trait Connection: Sized {
    fn try_limited() -> Result<Self, options::Error>;
    fn try_qpack(role: Role) -> Result<Self, options::Error>;
    fn try_validate(connect: Connect) -> Result<Self, options::Error>;
    fn try_bounded() -> Result<Self, options::Error>;
    fn try_request(
        stream: Id,
    ) -> Result<(Self, hold::Charge, Option<hold::Charge>, Option<Token>), Request>;
}

impl<'d> Connection for conn::Connection<'d> {
    fn try_limited() -> Result<Self, options::Error> {
        let mut config = Config::qpack();
        config.egress.wire_bytes = 2;
        Self::with_config(Role::Server, config)
    }
    fn try_qpack(role: Role) -> Result<Self, options::Error> {
        Self::with_config(role, Config::qpack())
    }
    fn try_validate(connect: Connect) -> Result<Self, options::Error> {
        Self::with_config(
            Role::Server,
            Config {
                protocol: Protocol {
                    extended_connect: connect,
                    ..Protocol::default()
                },
                ..Config::default()
            },
        )
    }
    fn try_bounded() -> Result<Self, options::Error> {
        Self::with_config(Role::Server, Config::single())
    }
    fn try_request(
        stream: Id,
    ) -> Result<(Self, hold::Charge, Option<hold::Charge>, Option<Token>), Request> {
        let mut sender = Self::with_role(Role::Client).map_err(Request::Config)?;
        let mut receiver = Self::with_role(Role::Server).map_err(Request::Config)?;
        let charge = Send::new(&mut sender)
            .headers(
                stream,
                [
                    literal!(entry(b":method", b"POST")),
                    literal!(entry(b":scheme", b"https")),
                    literal!(entry(b":authority", b"example.test")),
                    literal!(entry(b":path", b"/upload")),
                ],
                Boundary::More,
            )
            .map_err(Request::Connection)?;
        let write = sender
            .events()
            .poll_write()
            .map_err(Request::Connection)?
            .ok_or(Request::Write)?;
        let actual = write.charge();
        Receive::new(&mut receiver)
            .owned(stream, write.into_bytes(), Boundary::More)
            .map_err(Request::Connection)?;
        let event = match next_event(&mut receiver).map_err(Request::Connection)? {
            Some(Event::Headers { message, .. }) => Some(message.stream_id()),
            _ => None,
        };
        Ok((receiver, charge, actual, event))
    }
}

#[derive(Debug)]
pub enum Request {
    Config(options::Error),
    Connection(conn::Error),
    Write,
}
