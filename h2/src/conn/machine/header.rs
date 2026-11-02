use sark_core::{life::session, manifold::stream::Section};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{event, machine},
    stream,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) stream_id: stream::Nonzero,
    pub(crate) exchange: Option<stream::Key>,
    pub(crate) stage: Stage,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Stage {
    Initial,
    Other,
}

const _: () = assert!(core::mem::size_of::<Stage>() == core::mem::size_of::<bool>());
const _: () = assert!(core::mem::align_of::<Stage>() == core::mem::align_of::<bool>());

impl Header {
    pub(crate) fn read<R, F, C, A, E>(conn: &machine::Connection<R, F, C, A, E>) -> Option<Self>
    where
        R: crate::Role,
        F: rules::Policy,
        C: session::Storage,
        A: rules::Push,
        E: event::Evidence,
    {
        use crate::conn::ingress::headers::{Action, Kind};

        let pending = conn.ingress.headers.pending()?;
        let (section, action) = match pending.kind {
            Kind::Headers {
                section, action, ..
            } => (section, action),
            Kind::PushPromise { .. } => {
                return Some(Self {
                    stream_id: pending.stream_id,
                    exchange: None,
                    stage: Stage::Other,
                });
            }
        };
        let exchange = match action {
            Action::Deliver => conn.state.active_streams.store.key(pending.stream_id),
            Action::Reset(_) => None,
        };
        Some(Self {
            stream_id: pending.stream_id,
            exchange,
            stage: match section {
                Section::Initial => Stage::Initial,
                Section::Trailing => Stage::Other,
            },
        })
    }
}
