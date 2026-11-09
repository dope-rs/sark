use o3::buffer::storage;
use sark_core::life::session;
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{config, event, machine, registry},
    flow,
    frame::{self, numbers},
    stream,
};

/// Shared view of protocol bytes waiting for the final link.
#[repr(transparent)]
pub struct Output<
    'a,
    R: crate::Role,
    F: rules::Policy = rules::RetainRaw,
    C: session::Storage = storage::Shared,
    A: rules::Push = rules::RejectPush,
    E: event::Evidence = event::Retirement,
> {
    pub(in crate::conn::machine) conn: &'a machine::Connection<R, F, C, A, E>,
}

/// Shared view of negotiated connection settings.
#[repr(transparent)]
pub struct Settings<
    'a,
    R: crate::Role,
    F: rules::Policy = rules::RetainRaw,
    C: session::Storage = storage::Shared,
    A: rules::Push = rules::RejectPush,
    E: event::Evidence = event::Retirement,
> {
    pub(in crate::conn::machine) conn: &'a machine::Connection<R, F, C, A, E>,
}

/// Shared view of connection and stream lifecycle state.
#[repr(transparent)]
pub struct Status<
    'a,
    R: crate::Role,
    F: rules::Policy = rules::RetainRaw,
    C: session::Storage = storage::Shared,
    A: rules::Push = rules::RejectPush,
    E: event::Evidence = event::Retirement,
> {
    pub(in crate::conn::machine) conn: &'a machine::Connection<R, F, C, A, E>,
}

const _: () = {
    assert!(usize::BITS >= 64);
    assert!(
        isize::MAX as u128 + u32::MAX as u128 * (frame::HEADER_LEN + 4) as u128
            <= usize::MAX as u128
    );
};

impl<'a, R, F, C, A, E> Output<'a, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub fn first(self) -> &'a [u8] {
        let (first, _) = self.slices();
        first
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        let resets = self.conn.state.active_streams.reset_len() as u128;
        let pending = resets * (frame::HEADER_LEN + 4) as u128;
        (self.conn.egress.outbound.len() as u128 + pending) as usize
    }

    pub fn slices(self) -> (&'a [u8], &'a [u8]) {
        let outbound = &self.conn.egress.outbound;
        let limit = outbound.front_prefix();
        let (first, second) = outbound.bytes.as_slices();
        let left = first.len().min(limit);
        let right = second.len().min(limit - left);
        (&first[..left], &second[..right])
    }
}

impl<'a, R, F, C, A, E> Settings<'a, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub fn local(self) -> &'a config::Settings {
        &self.conn.state.settings.local
    }

    pub fn peer(self) -> &'a config::Settings {
        &self.conn.state.settings.peer
    }

    pub fn table_cap(&self) -> usize {
        self.conn.egress.headers.capacity()
    }

    pub fn table_limit(&self) -> usize {
        self.conn.egress.headers.table_limit()
    }
}

impl<R, F, C, A, E> Status<'_, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub fn send_window(&self) -> flow::Window {
        self.conn.state.flow.send_window
    }

    pub fn recv_window(&self) -> flow::Window {
        self.conn.state.flow.recv_window
    }

    pub fn recv_goaway(&self) -> Option<numbers::ErrorCode> {
        self.conn.state.lifecycle.goaway_received
    }

    pub fn is_goaway(&self) -> bool {
        self.conn.state.lifecycle.is_goaway()
    }

    pub fn is_draining(&self) -> bool {
        self.conn.state.lifecycle.is_draining()
    }

    pub fn is_closing(&self) -> bool {
        self.conn.state.lifecycle.is_closing()
    }

    pub fn has_stream(&self, id: stream::Id) -> bool {
        self.stream_key(id).is_some()
    }

    pub fn stream_key(&self, id: stream::Id) -> Option<stream::Key> {
        self.conn.state.active_streams.store.key(id.nonzero()?)
    }

    pub fn has_key(&self, key: stream::Key) -> bool {
        self.conn
            .state
            .active_streams
            .store
            .record_at(key)
            .is_some()
    }

    pub(crate) fn owns_exchange(&self, key: stream::Key) -> bool {
        self.conn.state.active_streams.store.owns_exchange(key)
    }

    pub fn capacity(&self) -> usize {
        self.conn.state.active_streams.store.capacity()
    }

    pub fn stream_state(&self, id: stream::Id) -> Option<stream::State> {
        self.record(id).map(|record| record.state)
    }

    pub fn active_len(&self) -> usize {
        self.conn.state.active_streams.store.active_len()
    }

    fn record(&self, id: stream::Id) -> Option<&registry::Active> {
        let store = &self.conn.state.active_streams.store;
        store
            .key(id.nonzero()?)
            .and_then(|key| store.record_at(key))
    }

    pub fn send_for(&self, id: stream::Id) -> Option<flow::Window> {
        self.record(id).map(|record| record.send_window)
    }

    pub fn recv_for(&self, id: stream::Id) -> Option<flow::Window> {
        self.record(id).map(|record| record.recv_window)
    }
}

const _: () = {
    use core::mem::{align_of, size_of};

    assert!(size_of::<Output<'static, crate::ClientRole>>() == size_of::<usize>());
    assert!(size_of::<Settings<'static, crate::ClientRole>>() == size_of::<usize>());
    assert!(size_of::<Status<'static, crate::ClientRole>>() == size_of::<usize>());
    assert!(align_of::<Output<'static, crate::ClientRole>>() == align_of::<usize>());
    assert!(align_of::<Settings<'static, crate::ClientRole>>() == align_of::<usize>());
    assert!(align_of::<Status<'static, crate::ClientRole>>() == align_of::<usize>());
};
