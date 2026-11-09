use core::ops::{self, ControlFlow};

use dope::core::BranchError;
use o3::buffer::{hold::evidence::Off, resident};
use sark_core::{
    body::credit,
    decode::{self, exec, request},
    manifold::{
        schedule::{self, typed},
        stream::Boundary,
    },
};
use sark_http::{field::metadata::section::rules, payload::owned, request::body::Ref};

use crate::{
    conn::{self, event, machine},
    frame::numbers,
    server::{
        self, response,
        state::exchange::{Deferred, Routed, Sync, admit, try_dispatch},
        sync::{
            self,
            book::{self, Control, Receiving, State, ingress::head::Begin, phase::Phase},
        },
    },
    stream,
};

pub(in crate::server::sync) struct Driver<'a, 'd: 'a, R: decode::Decode, B, D> {
    pub(super) connection: &'a mut machine::Connection<
        crate::ServerRole,
        R::Plan,
        resident::Shared<'d>,
        rules::RejectPush,
        event::Ignore,
    >,
    pub(super) router: &'a R,
    pub(super) limits: server::Config,
    pub(super) credit: &'a mut credit::raw::Account,
    pub(super) permit: B,
    pub(super) report: D,
}

impl<'a, 'd: 'a, R, B, D> Driver<'a, 'd, R, B, D>
where
    D: FnMut(conn::Error),
    R: decode::Decode,
    R::Prepared: request::Synchronous,
    <<R::Head as request::Head>::Start as request::Selection<R::Prepared>>::Deferred<
        Routed<'d, R::Prepared, R::Routes>,
    >: Deferred<'d, R::Prepared, R::Routes>,
{
    fn is_available(&mut self) -> bool {
        self.connection.output().len() < self.limits.max_outbound_bytes
    }

    fn finish_head(
        &mut self,
        stream: stream::Key,
        result: Result<ops::ControlFlow<response::Head<'d>, response::Receipt<'d>>, conn::Error>,
    ) -> Control<'d, R> {
        match result {
            Ok(ops::ControlFlow::Break(head)) => Self::retain(Phase::Head(head), Self::blocked()),
            Ok(ops::ControlFlow::Continue(receipt)) => match receipt.into_pending() {
                Some(pending) => self.start_body(stream, pending),
                None => typed::Control::Complete,
            },
            Err(error) => self.removed(stream, BranchError::First(error)),
        }
    }

    fn start_body(
        &mut self,
        stream: stream::Key,
        mut pending: response::Pending<'d>,
    ) -> Control<'d, R> {
        match pending.turn(self.connection, stream) {
            Ok(turn) => typed::Control::Fused {
                value: pending,
                turn,
            },
            Err(error) => self.removed(stream, BranchError::First(error)),
        }
    }

    const fn blocked() -> schedule::Turn<conn::DataBlock> {
        schedule::Turn::Blocked(conn::DataBlock::Egress)
    }

    const fn retain(phase: Phase<'d, R>, turn: schedule::Turn<conn::DataBlock>) -> Control<'d, R> {
        typed::Control::Control { value: phase, turn }
    }

    fn removed(
        &mut self,
        stream: stream::Key,
        error: BranchError<conn::Error, numbers::ErrorCode>,
    ) -> Control<'d, R> {
        let error = sync::response(error, &mut self.report);
        self.connection.streams().cancel(stream, error);
        typed::Control::Remove
    }

    pub(super) fn routed(
        &mut self,
        stream: stream::Key,
        routed: Routed<'d, R::Prepared, R::Routes>,
    ) -> Control<'d, R>
    where
        R: decode::Guarded,
    {
        let encoder = response::Encoder::new(self.connection, stream, Ref::from_slice(&[]));
        match admit::run::<R, _, Off, event::Ignore, _>(self.router, routed.into_state(), encoder) {
            Ok(ControlFlow::Continue(prepared)) => match Sync::new(prepared).take() {
                request::Body::Finished(request) => {
                    // SAFETY: Book::pump lends this account only to its runtime's
                    // driver; admission preserves the selected routed body's owner.
                    let ready = match unsafe { book::raw::try_body::<R>(request, self.credit) } {
                        Ok(ready) => ready,
                        Err(error) => return self.removed(stream, BranchError::Second(error)),
                    };
                    self.request(stream, ready)
                }
                request::Body::Open(request) => typed::Control::Idle(request),
            },
            Ok(ControlFlow::Break((reject, input))) => {
                // SAFETY: this rejection retains the runtime-selected route's body.
                if let Err(error) = unsafe { book::raw::discard(input, self.credit) } {
                    if let ControlFlow::Break(head) = reject {
                        head.discard(self.connection);
                    }
                    return self.removed(stream, BranchError::Second(error.code()));
                }
                self.finish_head(stream, Ok(reject))
            }
            Err((error, failed, input)) => {
                let error = sync::response(error, &mut self.report);
                // SAFETY: failed admission returns the same selected route's body.
                let error = match unsafe { book::raw::discard(input, self.credit) } {
                    Ok(()) => error,
                    Err(error) => error.code(),
                };
                self.removed(failed, BranchError::Second(error))
            }
        }
    }

    fn request(
        &mut self,
        stream: stream::Key,
        ready: request::Ready<exec::Sync, R::Prepared, owned::Segments<'d>>,
    ) -> Control<'d, R> {
        let reply =
            try_dispatch::<R, Off, event::Ignore, _>(ready, self.router, self.connection, stream);
        match reply {
            Ok(reply) => self.finish_head(stream, Ok(reply)),
            Err((error, failed)) => self.removed(failed, error),
        }
    }

    fn drive_data(
        &mut self,
        stream: stream::Key,
        pending: &mut response::Pending<'d>,
    ) -> Result<schedule::Turn<conn::DataBlock>, numbers::ErrorCode> {
        pending
            .turn(self.connection, stream)
            .map_err(|error| sync::response(BranchError::First(error), &mut self.report))
    }
}

impl<'a, 'd: 'a, R, B, D> typed::Settle<Phase<'d, R>, response::Pending<'d>, Receiving<'d, R>>
    for Driver<'a, 'd, R, B, D>
where
    D: FnMut(conn::Error),
    R: decode::Decode,
    R::Prepared: request::Synchronous,
    <<R::Head as request::Head>::Start as request::Selection<R::Prepared>>::Deferred<
        Routed<'d, R::Prepared, R::Routes>,
    >: Deferred<'d, R::Prepared, R::Routes>,
{
    type Key = stream::Key;
    type Error = numbers::ErrorCode;

    fn settle(
        &mut self,
        stream: Self::Key,
        phase: State<'d, R>,
        terminal: schedule::Terminal<Self::Error>,
    ) -> schedule::Settlement<State<'d, R>> {
        if matches!(&terminal, schedule::Terminal::Idle) && matches!(&phase, State::Idle(_)) {
            return schedule::Settlement::Keep(phase);
        }
        // SAFETY: Book::pump creates this driver solely for the runtime that
        // owns the terminal phase and retains the account beside that runtime.
        let (head, error) = match unsafe { book::raw::try_close(phase, self.credit) } {
            Ok(head) => (
                head,
                match terminal {
                    schedule::Terminal::Failed(error) => Some(error),
                    schedule::Terminal::Complete | schedule::Terminal::Idle => None,
                },
            ),
            Err(error) => (None, Some(error.code())),
        };
        if let Some(error) = error {
            self.connection.streams().cancel(stream, error);
        }
        if let Some(head) = head {
            head.discard(self.connection);
        }
        schedule::Settlement::Remove
    }
}

impl<'a, 'd: 'a, R, B, P, D> typed::Driver<Phase<'d, R>, response::Pending<'d>, Receiving<'d, R>>
    for Driver<'a, 'd, R, B, D>
where
    D: FnMut(conn::Error),
    B: FnMut() -> Option<P>,
    R: decode::Decode,
    R::Prepared: request::Synchronous,
    <R::Head as request::Head>::Start: Begin<'d, R>,
    <<R::Head as request::Head>::Start as request::Selection<R::Prepared>>::Deferred<
        Routed<'d, R::Prepared, R::Routes>,
    >: Deferred<'d, R::Prepared, R::Routes>,
{
    type Block = conn::DataBlock;
    type Permit = P;

    fn permit(&mut self) -> Option<Self::Permit> {
        self.is_available().then(&mut self.permit).flatten()
    }

    fn control(
        &mut self,
        _permit: Self::Permit,
        stream: Self::Key,
        phase: Phase<'d, R>,
    ) -> Control<'d, R> {
        if !matches!(&phase, Phase::Head(_)) && !self.connection.streams().headers().is_ready() {
            return Self::retain(phase, Self::blocked());
        }
        match phase {
            Phase::Routed(routed) => {
                <<R::Head as request::Head>::Start as Begin<'d, R>>::resume(self, stream, routed)
            }
            Phase::Request(request) => self.request(stream, request),
            Phase::Status(status) => match status.prepare(self.connection, stream, Boundary::End) {
                Ok(head) => {
                    let result = head.drive(self.connection);
                    self.finish_head(stream, result)
                }
                Err(error) if error.is_pressure() => {
                    Self::retain(Phase::Status(status), Self::blocked())
                }
                Err(error) => self.removed(stream, BranchError::First(error)),
            },
            Phase::Head(head) => {
                let result = head.drive(self.connection);
                self.finish_head(stream, result)
            }
        }
    }

    fn data(
        &mut self,
        _permit: Self::Permit,
        stream: stream::Key,
        pending: &mut response::Pending<'d>,
    ) -> Result<schedule::Turn<Self::Block>, Self::Error> {
        self.drive_data(stream, pending)
    }
}
