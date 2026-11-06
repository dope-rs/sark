use o3::buffer::storage;
use sark_core::{
    life::session,
    manifold::server::{self, drain},
};
use sark_http::field::metadata::section::rules;

use crate::{
    conn,
    conn::{event, machine},
};

mod raw;

/// One resident event, retaining exclusive access to its connection until consumed.
#[must_use]
#[repr(transparent)]
pub struct Incoming<'a, R, F, C, A, E>(&'a mut machine::Connection<R, F, C, A, E>)
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence;

impl<'a, R, F, C, A, E> Incoming<'a, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub(super) fn new(conn: &'a mut machine::Connection<R, F, C, A, E>) -> Option<Self> {
        (!conn.ingress.events.is_empty()).then_some(Self(conn))
    }
}

/// A server event that remains resident in its owning connection queue.
#[must_use]
#[repr(transparent)]
pub struct Ready<'a, F, C, E>(Incoming<'a, crate::ServerRole, F, C, rules::RejectPush, E>)
where
    F: rules::Policy,
    C: session::Storage,
    E: event::Evidence;

impl<'a, F, C, E> Ready<'a, F, C, E>
where
    F: rules::Policy,
    C: session::Storage,
    E: event::Evidence,
{
    pub(super) const fn new(
        ready: Incoming<'a, crate::ServerRole, F, C, rules::RejectPush, E>,
    ) -> Self {
        Self(ready)
    }

    pub(crate) fn data_stream(&self) -> Option<crate::stream::Key> {
        match self.0.as_ref() {
            event::Event::Data { stream_id, .. } => Some(*stream_id),
            _ => None,
        }
    }

    pub(crate) fn rejection(&self) -> Option<&event::Rejected> {
        match self.0.as_ref() {
            event::Event::StreamReset {
                validation: check, ..
            } => check.as_ref(),
            _ => None,
        }
    }
}

impl<F, C, E> drain::Ready for Ready<'_, F, C, E>
where
    F: rules::Policy,
    C: session::Storage,
    E: event::Evidence,
{
    type Item = server::Event<machine::Connection<crate::ServerRole, F, C, rules::RejectPush, E>>;

    fn take(self) -> Self::Item {
        match drain::Ready::take(self.0) {
            event::Event::Headers {
                stream_id: id,
                head,
                boundary,
                section,
            } => server::Event::Head {
                message: conn::Message::new(id, boundary),
                head,
                section,
            },
            event::Event::Data {
                stream_id: id,
                data,
                boundary,
            } => server::Event::Data {
                message: conn::Message::new(id, boundary),
                data,
            },
            event::Event::StreamReset {
                stream_id: id,
                error,
                retired,
                ..
            } => server::Event::Reset {
                stream: id,
                reason: error.reason(),
                retired,
            },
            event::Event::EgressRetired { write } => server::Event::Retired(write),
            event::Event::PrefaceComplete => server::Event::Established,
            _ => server::Event::Control,
        }
    }
}

const _: () = assert!(
    core::mem::size_of::<Ready<'static, rules::RetainRaw, storage::Shared, event::Retirement>>()
        == core::mem::size_of::<usize>()
);
