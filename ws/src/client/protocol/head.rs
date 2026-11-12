use std::ops;

use dope::manifold::client::session::codec;
use o3::buffer::{self, hold, resident};

use crate::{
    client::{Fault, protocol},
    fragment, inbound,
};

pub(in crate::client) enum Kind<'d> {
    Opened { accept: [u8; 28] },
    Text(buffer::storage::Str<resident::Shared<'d>>),
    AssembledText(String),
    Binary(resident::Shared<'d>),
    AssembledBinary(Vec<u8>),
    Pong(resident::Shared<'d>),
    Ping(resident::Shared<'d>),
    Close(protocol::raw::Close<'d>),
    Continuation,
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
            inbound::Prepared::Message(message) => match message.commit() {
                fragment::Message::WholeText(_) => Whole::Text,
                fragment::Message::WholeBinary(_) => Whole::Binary,
                fragment::Message::JoinedText(text) => {
                    return Ok(Self(Kind::AssembledText(text)));
                }
                fragment::Message::JoinedBinary(bytes) => {
                    return Ok(Self(Kind::AssembledBinary(bytes)));
                }
            },
            inbound::Prepared::Fragment(fragment) => {
                fragment.commit();
                return Ok(Self(Kind::Continuation));
            }
            inbound::Prepared::Pong(pong) => {
                pong.commit();
                Whole::Pong
            }
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
                Self(Kind::Text(text))
            }
            Whole::Binary => Self(Kind::Binary(payload)),
            Whole::Pong => Self(Kind::Pong(payload)),
            Whole::Ping => Self(Kind::Ping(payload)),
            Whole::Close(code) => {
                // SAFETY: prepare admitted this complete close payload and
                // its UTF-8 reason; retention preserves the exact bytes.
                let close = unsafe { protocol::raw::Close::admitted(payload, code) };
                Self(Kind::Close(close))
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
