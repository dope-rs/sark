use std::num::NonZero;

use dope_quic::wire::{stream::Id, varint};
use sark_core::manifold::stream::Boundary;
use sark_http::{self, field::metadata::section::rules};

use crate::{
    conn,
    conn::{
        io::ingress::{Parts, receive::Recv, stream::state},
        push,
    },
    frame,
};

#[derive(Default)]
// Keep the message tag after the request-sized prefix in table::State.
#[repr(C)]
pub(super) struct State {
    push_id: Option<NonZero<u64>>,
    pub(super) message: state::State,
}

impl State {
    pub(super) fn is_waiting(&self) -> bool {
        self.push_id.is_some() && self.message.method.is_none()
    }
}

pub(super) fn ingest<'d, P: rules::Policy, A: rules::Push>(
    parts: &mut Parts<'_, 'd, P, A>,
    id: Id,
    recv: &mut Recv<'d>,
    state: &mut State,
    boundary: Boundary,
    released: &mut usize,
) -> Result<Option<Id>, conn::Error> {
    let push_id = match state.push_id {
        Some(push_id) => {
            let push_id = push_id.get() - 1;
            if state.message.method.is_none() {
                state.message.method = parts.session.pushes.method(push_id);
            }
            push_id
        }
        None => {
            let (push_id, consumed) = match frame::decode_varint_source(&recv.buf) {
                Ok(value) => value,
                Err(
                    cause @ (frame::ParseError::NeedMore
                    | frame::ParseError::VarInt(varint::Error::Underflow { .. })),
                ) => {
                    return match boundary {
                        Boundary::End => Err(conn::Error::Incomplete {
                            stream_id: id.get(),
                            buffered: crate::Source::len(&recv.buf),
                            cause,
                        }),
                        Boundary::More => Ok(None),
                    };
                }
                Err(error) => return Err(conn::Error::Parse(error)),
            };
            push::check(push_id.get(), parts.session.max_push_id).map_err(conn::Error::Push)?;
            state.message.method = parts
                .session
                .pushes
                .receive(push_id, id)
                .map_err(conn::Error::Push)?;
            let push_id = push_id.get();
            // VarInt's 62-bit bound makes the one-based representation nonzero.
            state.push_id = NonZero::new(push_id + 1);
            recv.consume(consumed, released, &mut parts.ingress.budget)?;
            push_id
        }
    };
    if state.is_waiting() {
        return Ok(None);
    }
    state::ingest::<true, _, _>(
        parts,
        id,
        recv,
        &mut state.message,
        push_id,
        boundary,
        released,
    )
}
