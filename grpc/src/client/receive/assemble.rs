use o3::{
    alloc,
    buffer::{segments, storage},
};
use sark_core::{
    life::session,
    manifold::client::{Connection, domain::Plan, io::Read as _},
};
use sark_h2::{conn, stream};

use crate::{
    client::{
        config,
        receive::{
            self, Cancel, Fault, Phase, Response, StreamKey, UnaryKey, UnaryResult, budget, claims,
            policies, turn,
        },
        state::{calls, output},
    },
    protocol::section,
};

pub(in crate::client) struct Assembler<P, C = storage::Shared>
where
    C: session::Storage,
    P: Plan<C, Connection: Connection<Error = conn::Error>>,
{
    pub(super) segments: segments::Pool<C>,
    pub(super) claims: claims::Claims<C>,
    pub(super) budget: budget::Budget,
    pub(super) pending: Option<turn::Pending<P, C>>,
    pub(super) fault: Fault,
    pub(in crate::client) phase: Phase,
}

impl<P, C> Assembler<P, C>
where
    C: session::Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
{
    pub(in crate::client) fn try_new(limits: config::Limits) -> Result<Self, alloc::Error> {
        let input = limits.input();
        Ok(Self {
            segments: segments::Pool::try_new(segments::Plan {
                segments: input.segments(),
                chains: input.segments() / 2,
            })?,
            claims: claims::Claims::try_new(
                limits.unary_count(),
                input.done_count(),
                limits.call_count(),
                input.event_count(),
                limits.unary_count().get(),
            )?,
            budget: budget::Budget::new(input.bytes(), input.owner_count().get()),
            pending: None,
            fault: Fault::new(),
            phase: Phase::Open,
        })
    }

    pub(in crate::client) fn poll_unary(
        &mut self,
        key: UnaryKey,
    ) -> Result<receive::Progress<Result<UnaryResult<C>, crate::Status>>, crate::Status> {
        let delivery = match self.claims.poll_unary(key)? {
            receive::Progress::Ready(delivery) => delivery,
            receive::Progress::Pending(index) => return Ok(receive::Progress::Pending(index)),
        };
        self.fault.release(&mut self.budget, delivery.charge);
        self.take_error()?;
        Ok(receive::Progress::Ready(delivery.value))
    }

    pub(in crate::client) fn claim_unary(&mut self) -> Result<UnaryKey, crate::Status> {
        self.claims.claim_unary()
    }

    pub(in crate::client) fn cancel_unary(&mut self, key: UnaryKey) -> Cancel {
        match self.claims.cancel_unary(key) {
            Ok(claims::UnaryCancel::Missing) => Cancel::Missing,
            Ok(claims::UnaryCancel::Complete(charge)) => {
                self.fault.release(&mut self.budget, charge);
                Cancel::Complete(key.slot())
            }
            Err(error) => {
                self.fault.store(crate::Status::claim(error));
                Cancel::Missing
            }
        }
    }

    pub(in crate::client) fn claim_stream(&mut self) -> Result<StreamKey, crate::Status> {
        self.claims.claim_stream()
    }

    pub(in crate::client) fn poll_stream(
        &mut self,
        key: StreamKey,
    ) -> Result<receive::Progress<Result<Response<C>, crate::Status>>, crate::Status> {
        let delivery = match self.claims.poll_stream(key)? {
            receive::Progress::Ready(delivery) => delivery,
            receive::Progress::Pending(index) => return Ok(receive::Progress::Pending(index)),
        };
        self.fault.release(&mut self.budget, delivery.charge);
        self.take_error()?;
        Ok(receive::Progress::Ready(delivery.value))
    }

    pub(in crate::client) fn cancel_feed(&mut self, key: StreamKey) -> Cancel {
        match self
            .claims
            .cancel_feed(key, &mut self.fault, &mut self.budget)
        {
            Ok(cancel) => cancel,
            Err(error) => {
                self.fault.store(crate::Status::claim(error));
                Cancel::Missing
            }
        }
    }

    pub(super) fn cancel_reply(&mut self, response: calls::Response<C>) -> Cancel {
        let calls::Response {
            target, message, ..
        } = response;
        self.fault
            .release(&mut self.budget, message.map(|buffered| buffered.charge));
        match target {
            calls::Target::Unary(key) => self.cancel_unary(key),
            calls::Target::Streaming(key) => self.cancel_feed(key),
        }
    }

    pub(in crate::client) fn take_error(&mut self) -> Result<(), crate::Status> {
        self.fault.take()
    }

    pub(in crate::client) const fn is_blocked(&self) -> bool {
        self.pending.is_some()
    }

    /// Releases only this stream's pending event before its call is retired.
    pub(in crate::client) fn discard(&mut self, stream: P::Key) {
        let matches = match self.pending.as_ref() {
            Some(
                turn::Pending::Head {
                    stream: pending, ..
                }
                | turn::Pending::Data {
                    stream: pending, ..
                }
                | turn::Pending::Reset {
                    stream: pending, ..
                },
            ) => *pending == stream,
            Some(turn::Pending::GoAway { .. }) | None => false,
        };
        if matches {
            self.pending = None;
        }
    }

    pub(in crate::client) fn refill<R: policies::Policy<P, C>>(
        &mut self,
        conn: &mut P::Connection,
        calls: &mut calls::Store<P, C>,
        egress: &mut output::Output,
        policy: &mut R,
    ) {
        if self.fault.has_error() {
            return;
        }
        if (self.pending.is_some() || conn.has_event())
            && let Err(status) = self.drive(conn, calls, egress, Ok(()), policy)
        {
            self.fault.store(status);
            return;
        }
        if self.pending.is_none() && conn.is_blocked() {
            let result = conn.resume();
            if let Err(status) = self.drive(conn, calls, egress, result, policy) {
                self.fault.store(status);
            }
        }
    }

    pub(in crate::client) fn drive<R: policies::Policy<P, C>>(
        &mut self,
        conn: &mut P::Connection,
        calls: &mut calls::Store<P, C>,
        egress: &mut output::Output,
        result: Result<(), conn::Error>,
        policy: &mut R,
    ) -> Result<(), crate::Status> {
        self.take_error()?;
        turn::Turn::new(self, conn, calls, egress, policy).run(result)
    }
}
