use core::ops::ControlFlow;

use o3::buffer::storage;
use sark_core::{life::session, manifold::stream::Boundary};
use sark_http::{field, field::metadata::section::rules, payload::response::Method};

use crate::{
    conn::{self, event, machine, machine::ops::state, outputs, outputs::headers::Encoded},
    stream,
};

#[repr(transparent)]
pub struct Headers<
    'a,
    R: crate::Role,
    F: rules::Policy = rules::RetainRaw,
    C: session::Storage = storage::Shared,
    A: rules::Push = rules::RejectPush,
    E: event::Evidence = event::Retirement,
> {
    pub(super) conn: &'a mut machine::Connection<R, F, C, A, E>,
}

const _: () = {
    use core::mem::{align_of, size_of};

    assert!(size_of::<Headers<'static, crate::ClientRole>>() == size_of::<usize>());
    assert!(align_of::<Headers<'static, crate::ClientRole>>() == align_of::<usize>());
};

impl<R, F, C, A, E> Headers<'_, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub fn trail<S: field::Source>(
        &mut self,
        id: stream::Id,
        source: S,
    ) -> Result<(), conn::Error> {
        let stream = self
            .conn
            .status()
            .stream_key(id)
            .ok_or(conn::Error::UnknownStream(id))?;
        self.trail_key(stream, source)
    }

    pub fn trail_key<S: field::Source>(
        &mut self,
        stream: stream::Key,
        source: S,
    ) -> Result<(), conn::Error> {
        let result = self.try_send(stream, source, Boundary::End)?;
        self.finish(stream, result)
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.conn.egress.headers.is_idle()
    }

    fn prepared<S: field::Source>(
        &mut self,
        key: stream::Key,
        source: S,
        boundary: Boundary,
    ) -> Result<Encoded<stream::Sending>, conn::Error> {
        if !self.conn.egress.headers.is_idle() {
            return Err(conn::Error::HeaderBusy);
        }
        let machine::Connection {
            egress,
            state:
                state::State {
                    settings,
                    active_streams: active,
                    ..
                },
            ..
        } = &mut *self.conn;
        let encoder = &mut egress.headers;
        let send = {
            let record = active
                .store
                .active_mut(key)
                .ok_or(conn::Error::InactiveStream(key))?;
            match stream::Sending::new(record.state, boundary) {
                Ok(send) => send,
                Err(error) => return Err(conn::Error::Transition(key.id(), error)),
            }
        };
        encoder.prepare(
            source,
            &settings.peer,
            stream::raw::Marked::new(key, boundary),
            send,
            None,
        )
    }

    pub(crate) fn prepare<S: field::Source>(
        &mut self,
        key: stream::Key,
        source: S,
        boundary: Boundary,
    ) -> Result<(), conn::Error> {
        let prepared = self.prepared(key, source, boundary)?;
        self.conn.egress.headers.retain(prepared);
        Ok(())
    }

    pub(super) fn try_send<S: field::Source>(
        &mut self,
        key: stream::Key,
        source: S,
        boundary: Boundary,
    ) -> Result<ControlFlow<outputs::Full>, conn::Error> {
        let prepared = self.prepared(key, source, boundary)?;
        self.publish(prepared)
    }

    fn publish(
        &mut self,
        prepared: Encoded<stream::Sending>,
    ) -> Result<ControlFlow<outputs::Full>, conn::Error> {
        let key = prepared.stream();
        let machine::Connection { egress, state, .. } = &mut *self.conn;
        let record = match state.active_streams.store.active_mut(key) {
            Some(record) => record,
            None => {
                egress.headers.rollback(prepared);
                return Err(conn::Error::InactiveStream(key));
            }
        };
        let outputs::Egress { outbound, headers } = egress;
        let send = match headers.try_commit(outbound, prepared)? {
            ControlFlow::Continue(send) => send,
            ControlFlow::Break((prepared, full)) => {
                headers.retain(prepared);
                return Ok(ControlFlow::Break(full));
            }
        };
        let next = send.apply(record.state);
        record.state = next;
        if next == stream::State::Closed {
            state.active_streams.remove_key(key, false);
        }
        Ok(ControlFlow::Continue(()))
    }

    pub(crate) fn try_commit(
        &mut self,
        key: stream::Key,
    ) -> Result<ControlFlow<outputs::Full>, conn::Error> {
        match self.conn.egress.headers.take(key) {
            Some(prepared) => self.publish(prepared),
            None if self
                .conn
                .state
                .active_streams
                .store
                .active_mut(key)
                .is_none() =>
            {
                Err(conn::Error::InactiveStream(key))
            }
            None => Err(conn::Error::HeaderBusy),
        }
    }

    pub(crate) fn commit(&mut self, key: stream::Key) -> Result<(), conn::Error> {
        let result = self.try_commit(key)?;
        self.finish(key, result)
    }

    fn finish(
        &mut self,
        key: stream::Key,
        result: ControlFlow<outputs::Full>,
    ) -> Result<(), conn::Error> {
        match result {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(full) => {
                self.discard(key);
                Err(full.into())
            }
        }
    }

    pub(crate) fn discard(&mut self, key: stream::Key) {
        self.conn.egress.headers.discard(key);
    }
}

impl<F, C, A, E> Headers<'_, crate::ClientRole, F, C, A, E>
where
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub fn request<S: field::Source>(
        &mut self,
        source: S,
        boundary: Boundary,
    ) -> Result<stream::Id, conn::Error> {
        self.request_key(source, boundary).map(stream::Key::id)
    }

    pub fn request_key<S: field::Source>(
        &mut self,
        source: S,
        boundary: Boundary,
    ) -> Result<stream::Key, conn::Error> {
        if self.conn.state.lifecycle.goaway_received.is_some()
            || self.conn.state.lifecycle.is_goaway()
        {
            return Err(conn::Error::StreamGoneAway);
        }
        let limit = match self.conn.state.settings.peer.max_concurrent_streams {
            Some(limit) => limit as usize,
            None => usize::MAX,
        };
        let next = self
            .conn
            .state
            .stream_history
            .next_local()
            .ok_or(conn::Error::StreamIdExhausted)?;
        let proof = next.id();
        let send = self.conn.state.settings.peer.initial_window_size;
        let recv = self.conn.state.settings.local.initial_window_size;
        let active = self.conn.state.active_streams.store.local_len();
        if active >= limit {
            return Err(conn::Error::StreamLimit { active, limit });
        }
        let vacant = self
            .conn
            .state
            .active_streams
            .store
            .vacant(proof)
            .map_err(conn::Error::Admission)?;
        let key = vacant.insert(
            stream::Lifecycle::open_local(boundary),
            send,
            recv,
            0,
            false,
        );
        let outputs::Egress {
            outbound,
            headers: encoder,
        } = &mut self.conn.egress;
        let method = match encoder.encode(
            outbound,
            key,
            source,
            boundary,
            &self.conn.state.settings.peer,
        ) {
            Ok(method) => method,
            Err(error) => {
                self.conn.state.active_streams.remove_key(key, false);
                return Err(error);
            }
        };
        self.conn
            .state
            .active_streams
            .store
            .active_mut(key)
            .ok_or(conn::Error::InactiveStream(key))?
            .method = Some(method);
        next.commit();
        Ok(key)
    }
}

impl<'a, F, C, A, E> Headers<'a, crate::ServerRole, F, C, A, E>
where
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub(crate) fn method(&self, key: stream::Key) -> Result<Method, conn::Error> {
        self.conn
            .state
            .active_streams
            .store
            .record_at(key)
            .and_then(|record| record.method)
            .ok_or(conn::Error::InactiveStream(key))
    }

    pub fn respond<S: field::Source>(
        &mut self,
        id: stream::Id,
        source: S,
        boundary: Boundary,
    ) -> Result<(), conn::Error> {
        let stream = self
            .conn
            .status()
            .stream_key(id)
            .ok_or(conn::Error::UnknownStream(id))?;
        self.respond_key(stream, source, boundary)
    }

    pub fn respond_key<S: field::Source>(
        &mut self,
        stream: stream::Key,
        source: S,
        boundary: Boundary,
    ) -> Result<(), conn::Error> {
        let result = self.try_send(stream, source, boundary)?;
        self.finish(stream, result)
    }
}
