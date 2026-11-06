use o3::buffer::resident;
use sark_core::{
    body::credit,
    decode::{self, exec, request},
    manifold::{
        schedule::{self, typed},
        server::Event,
        stream::{Boundary, Message as _, Section},
    },
};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{self, event, machine},
    frame::numbers,
    server::{
        self, payload,
        payload::Buffer,
        state::exchange::{Deferred, Routed},
        sync::book::{self, Book, Next, State, phase::Phase},
    },
    stream,
};

type Connection<'d, P> = machine::Connection<
    crate::ServerRole,
    P,
    resident::Shared<'d>,
    rules::RejectPush,
    event::Ignore,
>;
type Incoming<'d, P> = Event<Connection<'d, P>>;
pub(in crate::server::sync) mod head;

pub(in crate::server::sync) struct Ingress<'a, 'd: 'a, R: decode::Decode> {
    book: &'a mut Book<'d, R>,
    connection: &'a mut Connection<'d, R::Plan>,
}

impl<'a, 'd: 'a, R> Ingress<'a, 'd, R>
where
    R: decode::Decode,
    R::Prepared: request::Synchronous,
    <<R::Head as request::Head>::Start as request::Selection<R::Prepared>>::Deferred<
        Routed<'d, R::Prepared, R::Routes>,
    >: Deferred<'d, R::Prepared, R::Routes>,
{
    pub(in crate::server::sync) fn new(
        book: &'a mut Book<'d, R>,
        conn: &'a mut Connection<'d, R::Plan>,
    ) -> Self {
        Self {
            book,
            connection: conn,
        }
    }

    fn try_receive(
        &mut self,
        stream: stream::Key,
        input: request::Admitted<exec::Sync, R::Prepared, Buffer<'d>>,
    ) -> Result<(), request::Admitted<exec::Sync, R::Prepared, Buffer<'d>>> {
        let Some(slot) = self.book.runtime.vacant(stream) else {
            return Err(input);
        };
        slot.store(State::Idle(input));
        Ok(())
    }

    fn discard(&mut self, stream: stream::Key) -> Result<(), payload::Error> {
        if let Some(phase) = self.book.runtime.remove(stream) {
            // SAFETY: the phase is removed from the runtime beside this account.
            unsafe { book::raw::close(phase, &mut self.book.credit, self.connection) }?;
        }
        Ok(())
    }

    fn reset(&mut self, stream: stream::Key, error: numbers::ErrorCode) {
        let error = match self.discard(stream) {
            Ok(()) => error,
            Err(error) => error.code(),
        };
        self.connection.streams().cancel(stream, error);
    }

    fn reject(&mut self, failure: schedule::Insert<stream::Key, Next<'d, R>>) {
        let (stream, error) = match &failure {
            schedule::Insert::Full { stream, .. } => (*stream, numbers::ErrorCode::EnhanceYourCalm),
            schedule::Insert::Occupied { stream, .. }
            | schedule::Insert::Excluded { stream, .. } => {
                (*stream, numbers::ErrorCode::InternalError)
            }
        };
        self.reset(stream, error);
    }

    fn admit(&mut self, stream: stream::Key, phase: Phase<'d, R>, limits: server::Config) {
        let block = match &phase {
            Phase::Routed(_) => Some(conn::DataBlock::Egress),
            _ if self.connection.output().len() >= limits.max_outbound_bytes => {
                Some(conn::DataBlock::Egress)
            }
            _ => None,
        };
        if let Err(failure) = self
            .book
            .runtime
            .try_insert(stream, typed::Next::Control(phase, block))
        {
            self.reject(failure);
        }
    }

    fn data(
        &mut self,
        stream: stream::Key,
        data: event::DataPayload<resident::Shared<'d>>,
        boundary: Boundary,
        limits: server::Config,
    ) {
        let Some(mut entry) = self.book.runtime.entry(stream) else {
            data.discard();
            return;
        };
        let pushed = match &mut entry {
            // SAFETY: this entry and account are selected from the same table.
            typed::Entry::Control(entry) => unsafe {
                book::raw::try_push(entry.get_mut(), &mut self.book.credit, data, limits)
            },
            typed::Entry::Idle(entry) => {
                let input = entry.get_mut();
                let limit = input.body().max_body();
                Ok(self.book.credit.scope(|mut ledger| {
                    // SAFETY: this body is selected from the runtime paired with
                    // this account; the exclusive loan cannot leave this scope.
                    let mut body = unsafe { payload::raw::Loan::new(input.body_mut(), &ledger) };
                    body.try_push(
                        &mut ledger,
                        credit::Disabled::new(),
                        data,
                        limits.request(limit),
                        limits.connection(),
                    )
                }))
            }
            typed::Entry::Data(_) => Err(data),
        };
        match pushed {
            Ok(payload::Push::Accepted) => {}
            Ok(payload::Push::Rejected { input, error }) => {
                input.discard();
                // SAFETY: removal consumes the same entry selected beside this account.
                let error = unsafe {
                    book::raw::close(entry.remove(), &mut self.book.credit, self.connection)
                }
                .err()
                .unwrap_or(error)
                .code();
                self.connection.streams().cancel(stream, error);
                return;
            }
            Err(data) => {
                data.discard();
                return;
            }
        }
        if matches!(boundary, Boundary::End) {
            // SAFETY: DATA retained the original occupied entry and its account.
            unsafe {
                book::raw::finish(
                    entry,
                    &mut self.book.credit,
                    self.connection,
                    stream,
                    limits,
                )
            };
        }
    }

    fn finish(&mut self, stream: stream::Key, limits: server::Config) {
        if let Some(entry) = self.book.runtime.entry(stream) {
            // SAFETY: this entry and account are selected from the same table.
            unsafe {
                book::raw::finish(
                    entry,
                    &mut self.book.credit,
                    self.connection,
                    stream,
                    limits,
                )
            };
        }
    }

    pub(in crate::server::sync) fn run(
        &mut self,
        router: &R,
        limits: server::Config,
        mut next: impl FnMut(&mut Connection<'d, R::Plan>) -> Option<Incoming<'d, R::Plan>>,
        mut report: impl FnMut(conn::Error),
    ) where
        Self: head::Begin<<R::Head as request::Head>::Start, Router = R>,
    {
        while let Some(event) = next(self.connection) {
            match event {
                Event::Head {
                    message: frame,
                    head,
                    section: part,
                } => {
                    let stream = frame.key();
                    let boundary = frame.boundary();
                    if matches!(part, Section::Trailing) {
                        if matches!(boundary, Boundary::End) {
                            self.finish(stream, limits);
                        }
                        continue;
                    }
                    let head = match head.try_request() {
                        Ok(head) => head,
                        Err(_) => {
                            self.reset(stream, numbers::ErrorCode::ProtocolError);
                            continue;
                        }
                    };
                    head::Begin::begin(self, router, stream, head, boundary, limits, &mut report);
                }
                Event::Data {
                    message: frame,
                    data,
                } => self.data(frame.key(), data, frame.boundary(), limits),
                Event::Reset { stream, .. } => {
                    let result = self.discard(stream);
                    if let Err(error) = result {
                        self.connection.streams().cancel(stream, error.code());
                    }
                }
                Event::Retired(retired) => match retired {},
                Event::Control | Event::Established => {}
            }
        }
    }
}
