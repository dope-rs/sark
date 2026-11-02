use std::mem;

use dope::core::driver::schedule::clock;
use o3::{
    alloc,
    base::time::Positive,
    buffer::{resident, segments, storage},
    depot::{capacity, index::set},
};
use sark_core::{
    deadline::{self, Book},
    manifold::{self, client::event::Reset, schedule, server, server::app::Capacity},
};
use sark_h2::stream::Key;

use crate::server::{
    calls,
    config::Limits,
    egress,
    runtime::{
        run::Fault,
        state::{self, active, lanes},
    },
    service::timers,
};

pub(in crate::server::runtime) struct Session<'r, 'd, T, P: timers::Policy, S>
where
    S: server::Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
{
    pub(in crate::server::runtime) calls:
        calls::Store<'r, S::Key, T, resident::Shared<'d>, S::Permit, S::Block>,
    pub(in crate::server::runtime) egress: egress::Store<S::Key>,
    pub(in crate::server::runtime) segments: segments::Pool<resident::Shared<'d>>,
    pub(in crate::server::runtime) output_deadlines: P::State<'r, 'd>,
    pub(in crate::server::runtime) work: lanes::Schedule,
    pub(in crate::server::runtime) egress_released: bool,
    deadline_capacity: capacity::Count,
    call_deadlines: Option<deadline::Exact<Key, clock::Deadline<'d>>>,
}

impl<'r, 'd, T, P, S> Session<'r, 'd, T, P, S>
where
    P: state::Timing<'d>,
    S: server::Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
{
    pub(in crate::server::runtime) fn retire(&mut self, conn: &mut S) {
        for index in 0..self.calls.records.capacity() {
            let Some(stream) = self.calls.records.key_at(index) else {
                continue;
            };
            self.remove(conn, stream);
        }
        self.egress_released = false;
    }

    fn remove(&mut self, conn: &mut S, stream: S::Key) {
        match self.egress.remove(stream, conn) {
            Ok(released) => self.release(released),
            Err(
                Fault::Accounting
                | Fault::Lane(..)
                | Fault::Credit(..)
                | Fault::Protocol
                | Fault::Response(_),
            ) => conn.fail(),
        }
        self.remove_call(conn, stream);
    }

    fn release(&mut self, released: egress::Release) {
        if released.prepared {
            self.calls.records.wake_egress();
        }
        self.work.released |= released.output;
    }

    fn remove_call(&mut self, conn: &mut S, stream: S::Key) {
        self.cancel_time(stream);
        match self.calls.try_remove(stream) {
            Ok(()) => {}
            Err(
                calls::Error::Call(..)
                | calls::Error::Credit(..)
                | calls::Error::Chunks(..)
                | calls::Error::Lane(..)
                | calls::Error::State,
            ) => conn.fail(),
        }
    }
}

impl<'r, 'd, T, P, S> Session<'r, 'd, T, P, S>
where
    P: state::Timing<'d>,
    S: server::Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
{
    pub(in crate::server::runtime::state) fn try_new(
        limits: &Limits,
        cap: Capacity,
        times: P::State<'r, 'd>,
    ) -> Result<Self, alloc::Error> {
        let input = limits.input();
        let output = limits.output();
        let chunks = capacity::Count::new(cap.data());
        Ok(Self {
            calls: calls::Store::try_new(cap.streams(), chunks, input.count())?,
            egress: egress::Store::try_new(
                cap.streams(),
                capacity::Count::new(output.messages.get()),
                output.wire_bytes(),
                limits.message_bytes,
            )?,
            segments: segments::Pool::try_new(segments::Plan {
                segments: input.segments(),
                chains: input.segments() / 2,
            })?,
            output_deadlines: times,
            work: lanes::Schedule::default(),
            egress_released: false,
            deadline_capacity: cap.streams(),
            call_deadlines: None,
        })
    }
}

impl<'r, 'd, T, P, S> Session<'r, 'd, T, P, S>
where
    P: state::Timing<'d>,
    S: server::Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
{
    pub(in crate::server::runtime) fn try_deadline(
        &mut self,
        stream: Key,
        now: clock::Moment<'d>,
        timeout: Positive,
    ) -> Result<(), crate::Status> {
        let at = now.try_add(timeout.get())?;
        let book = match &mut self.call_deadlines {
            Some(book) => book,
            slot @ None => slot.insert(deadline::Exact::try_new(self.deadline_capacity)?),
        };
        book.schedule(
            stream,
            deadline::Scheduled::expire(at, deadline::Phase::Total),
        )?;
        Ok(())
    }

    pub(in crate::server::runtime) fn deadline(&self) -> Option<clock::Deadline<'d>> {
        self.call_deadlines
            .as_ref()?
            .peek()
            .map(|(_, item)| item.at)
    }

    pub(in crate::server::runtime) fn pop_deadline(
        &mut self,
        now: clock::Moment<'d>,
    ) -> Option<Key> {
        self.call_deadlines
            .as_mut()?
            .pop_due(now.into())
            .map(|item| item.key)
    }

    fn cancel_time(&mut self, stream: Key) {
        if let Some(book) = &mut self.call_deadlines {
            book.cancel(stream);
        }
    }

    /// Ends the application half of a call while preserving any already
    /// committed response owner until the egress driver releases it.
    pub(in crate::server::runtime) fn complete(&mut self, conn: &mut S, entry: active::Entry<'r>) {
        let stream = entry.key();
        match self.calls.complete(stream) {
            Ok(()) => {}
            Err(
                calls::Error::Call(..)
                | calls::Error::Credit(..)
                | calls::Error::Chunks(..)
                | calls::Error::Lane(..)
                | calls::Error::State,
            ) => conn.fail(),
        }
    }

    pub(in crate::server::runtime) fn poll_output<'a>(
        &mut self,
        active: &'a active::Signals,
    ) -> Option<active::Entry<'r>> {
        let entry = match active.pop::<1>(self.work.output) {
            Some(entry) => entry,
            None => {
                let entry = active.blocked(self.work.waiting, self.work.retries)?;
                let index = entry.index().raw();
                let rows = if index >= self.work.waiting {
                    index - self.work.waiting + 1
                } else {
                    active.capacity() - self.work.waiting + index + 1
                };
                self.work.retries -= rows;
                entry.clear(set::Id::at::<2>());
                let next = index + 1;
                self.work.waiting = if next == active.capacity() { 0 } else { next };
                entry
            }
        };
        let index = entry.index().raw();
        self.work.output = if index + 1 == active.capacity() {
            0
        } else {
            index + 1
        };
        self.calls.records.entry_at(index)
    }

    pub(in crate::server::runtime) fn poll_app<'a>(
        &mut self,
        active: &'a active::Signals,
    ) -> Option<active::Entry<'r>> {
        let entry = active.pop::<0>(self.work.semantic)?;
        let index = entry.index().raw();
        self.work.semantic = if index + 1 == active.capacity() {
            0
        } else {
            index + 1
        };
        self.calls.records.entry_at(index)
    }

    pub(in crate::server::runtime) fn discard(&mut self, entry: active::Entry<'r>, conn: &mut S) {
        let stream = entry.key();
        if self.calls.records.key_at(manifold::raw::Key::slot(stream)) != Some(stream) {
            return;
        }
        P::cancel(&mut self.output_deadlines, entry);
        self.remove(conn, stream);
    }

    pub(in crate::server::runtime) fn drive(
        &mut self,
        conn: &mut S,
        egress: schedule::Egress,
        active: &active::Signals,
        sink: &mut impl FnMut(Key, egress::Error<S::Error>),
        mut cancel: impl FnMut(active::Entry<'r>, &mut T),
    ) -> (schedule::Report, active::Edge) {
        let times = &mut self.output_deadlines;
        let book = &mut self.call_deadlines;
        let (report, released) = self
            .calls
            .drive(&mut self.egress, conn, egress, sink, |record| {
                let entry = record.entry;
                if let Some(book) = book {
                    book.cancel(entry.key());
                }
                P::cancel(times, entry);
                if let Some(call) = record.handler_mut() {
                    cancel(entry, call);
                }
            });
        self.release(released);
        let edge = if mem::take(&mut self.work.released) && active.has_blocked() {
            self.work.retries = active.capacity();
            active::Edge::Wake
        } else {
            active::Edge::None
        };
        (report, edge)
    }

    pub(in crate::server::runtime) fn send_error(
        &mut self,
        conn: &mut S,
        status: crate::Status,
        entry: active::Entry<'r>,
        sink: &mut impl FnMut(Key, egress::Error<S::Error>),
    ) {
        let stream = entry.key();
        match self.calls.records.key_at(manifold::raw::Key::slot(stream)) {
            Some(current) if current != stream => {
                return conn.reset(stream, Reset::Internal);
            }
            Some(_) => {}
            None => {
                if let Err(error) = self.calls.records.try_reserve(entry) {
                    conn.reset(stream, Reset::Internal);
                    return sink(stream, egress::Error::Fault(Fault::Response(error.into())));
                }
            }
        }
        self.cancel_time(stream);
        P::cancel(&mut self.output_deadlines, entry);
        let (released, retained) = match self.egress.fail(stream, conn, status) {
            Ok(next) => next,
            Err(fault) => {
                self.discard(entry, conn);
                conn.reset(stream, Reset::Internal);
                return sink(stream, egress::Error::Fault(fault));
            }
        };
        self.release(released);
        if !retained {
            self.remove_call(conn, stream);
            return conn.reset(stream, Reset::Internal);
        }
        if let Err(error) = self.calls.reject(stream) {
            self.discard(entry, conn);
            conn.reset(stream, Reset::Internal);
            sink(
                stream,
                egress::Error::Fault(Fault::Response(error.status())),
            );
        }
    }
}
