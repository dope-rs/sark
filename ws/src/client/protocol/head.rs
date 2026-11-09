use std::ops;

use dope::manifold::client::session::codec;
use o3::buffer::{self, hold, resident};

use crate::{
    client::{Fault, Message, protocol},
    fragment, inbound,
};

/// One message whose payload outlives the exact decode input it was admitted from.
pub(in crate::client) enum Frame<'d> {
    /// A whole frame whose exact range was validated as UTF-8 and retained.
    Text(Text<'d>),
    /// A whole frame's bytes, or the fragment assembler's joined owner.
    Binary(Bytes<'d>),
    /// A pong whose payload the retained owner still holds.
    Pong(resident::Shared<'d>),
}

impl<'d> Frame<'d> {
    /// Borrows the message for the one handler call this frame was decoded for.
    pub(in crate::client) fn message(&self) -> Message<'_> {
        match self {
            Self::Text(text) => Message::Text(text.as_str()),
            Self::Binary(bytes) => Message::Binary(bytes.as_slice()),
            Self::Pong(bytes) => Message::Pong(bytes.as_slice()),
        }
    }
}

/// Text that outlives the exact input its validation observed.
pub(in crate::client) enum Text<'d> {
    /// A whole frame's exact range, admitted as UTF-8 and retained.
    Retained(buffer::storage::Str<resident::Shared<'d>>),
    /// The fragment assembler's joined owner.
    Assembled(String),
}

impl<'d> Text<'d> {
    pub(in crate::client) fn as_str(&self) -> &str {
        match self {
            Self::Retained(text) => text.as_str(),
            Self::Assembled(text) => text.as_str(),
        }
    }
}

/// Binary that outlives the exact input its admission observed.
pub(in crate::client) enum Bytes<'d> {
    /// A whole frame's exact range retained into the response resident domain.
    Retained(resident::Shared<'d>),
    /// The fragment assembler's joined owner.
    Assembled(Vec<u8>),
}

impl<'d> Bytes<'d> {
    pub(in crate::client) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Retained(bytes) => bytes.as_ref(),
            Self::Assembled(bytes) => bytes.as_slice(),
        }
    }
}

/// One mandatory control frame whose exact reply the peer is owed.
pub(in crate::client) enum Control<'d> {
    /// A ping whose exact payload is echoed by its pong reply.
    Ping(resident::Shared<'d>),
    /// A close whose exact payload is echoed by its close reply.
    Close(protocol::raw::Close<'d>),
}

pub(in crate::client) enum Kind<'d> {
    Opened { accept: [u8; 28] },
    Frame(Frame<'d>),
    Continuation,
    Control(Control<'d>),
}

#[doc(hidden)]
pub struct Head<'d>(pub(in crate::client) Kind<'d>);

impl<'d> Head<'d> {
    pub(super) fn frame<S: buffer::Cursor + hold::raw::Retain<'d>>(
        input: &codec::Input<'_, '_, 'd, S>,
        prepared: inbound::Prepared<'_, '_, inbound::ServerPeer>,
        range: ops::Range<usize>,
    ) -> Result<Self, Fault> {
        let kind = match prepared {
            inbound::Prepared::Ready(ready) => match ready.commit() {
                inbound::ReadyEvent::Data(fragment::Push::DirectText(_)) => Whole::Text,
                inbound::ReadyEvent::Data(fragment::Push::DirectBinary(_)) => Whole::Binary,
                inbound::ReadyEvent::Data(fragment::Push::AssembledText(text)) => {
                    return Ok(Self(Kind::Frame(Frame::Text(Text::Assembled(text)))));
                }
                inbound::ReadyEvent::Data(fragment::Push::AssembledBinary(bytes)) => {
                    return Ok(Self(Kind::Frame(Frame::Binary(Bytes::Assembled(bytes)))));
                }
                inbound::ReadyEvent::Data(fragment::Push::NeedMore) => {
                    return Ok(Self(Kind::Continuation));
                }
                inbound::ReadyEvent::Pong(_) => Whole::Pong,
            },
            inbound::Prepared::Required(need) => match need.defer().event {
                inbound::Event::Ping(_) => Whole::Ping,
                inbound::Event::Close(close) => Whole::Close(close.code()),
            },
        };
        let payload = input
            .try_plan(range)
            .map_err(|error| Fault::Retention(hold::Error::Range(error)))?
            .bind(PAYLOAD)
            .try_into()
            .map_err(Fault::Retention)?;
        let head = match kind {
            Whole::Text => {
                // SAFETY: prepare validated this exact immutable payload
                // as UTF-8; retention preserves its exact range and bytes.
                let text = unsafe { buffer::raw::utf8(payload) };
                Self(Kind::Frame(Frame::Text(Text::Retained(text))))
            }
            Whole::Binary => Self(Kind::Frame(Frame::Binary(Bytes::Retained(payload)))),
            Whole::Pong => Self(Kind::Frame(Frame::Pong(payload))),
            Whole::Ping => Self(Kind::Control(Control::Ping(payload))),
            Whole::Close(code) => {
                // SAFETY: prepare admitted this complete close payload and
                // its UTF-8 reason; retention preserves the exact bytes.
                let close = unsafe { protocol::raw::Close::admitted(payload, code) };
                Self(Kind::Control(Control::Close(close)))
            }
        };
        Ok(head)
    }
}

/// The exact whole-frame case one admitted payload stands for.
enum Whole {
    Text,
    Binary,
    Ping,
    Pong,
    Close(Option<u16>),
}

/// Whole-frame payloads are retained into the response resident domain.
const PAYLOAD: hold::Policy = hold::Policy::share();
