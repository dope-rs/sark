use o3::{buffer::segments, depot::capacity};
use receive::budget::Admit;
use sark_core::{
    life::session,
    manifold::{
        client::{Connection, domain::Plan},
        stream::Boundary,
    },
};
use sark_h2::{conn, stream};

use crate::{
    Reason,
    client::{
        receive::{self, policies, turn},
        state::calls,
    },
    message,
    protocol::section,
};

enum Publish<C: session::Storage> {
    Complete,
    Blocked {
        message: message::Frame<C>,
        status: crate::Status,
    },
    Rejected(crate::Status),
}

enum Resource {
    Segments,
    Chains,
}

pub(super) struct Data<'a, 'turn, P, C, R>
where
    C: session::Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
    R: policies::Policy<P, C>,
{
    turn: &'a mut turn::Turn<'turn, P, C, R>,
    stream: P::Key,
    data: message::DataChunk<C, P::Permit>,
    boundary: Boundary,
    ready: Option<message::Frame<C>>,
}

impl<'a, 'turn, P, C, R> Data<'a, 'turn, P, C, R>
where
    C: session::Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
    R: policies::Policy<P, C>,
{
    pub(super) fn new(
        turn: &'a mut turn::Turn<'turn, P, C, R>,
        stream: P::Key,
        data: message::DataChunk<C, P::Permit>,
        boundary: Boundary,
        ready: Option<message::Frame<C>>,
    ) -> Self {
        Self {
            turn,
            stream,
            data,
            boundary,
            ready,
        }
    }

    pub(super) fn run(mut self) -> Result<turn::Step<P, C>, crate::Status> {
        let response = self
            .turn
            .calls
            .response_mut(self.stream)
            .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::EventUnknown))?;
        let http = response.http_status;
        let discard = http.is_some() && !response.grpc;
        if discard && matches!(self.boundary, Boundary::More) {
            return Ok(turn::Step::Complete);
        }
        loop {
            if !self
                .turn
                .assembler
                .claims
                .try_ready(response.target.mode())?
            {
                return Ok(self.blocked());
            }
            if discard {
                break;
            }
            let message = match self.ready.take() {
                Some(message) => message,
                None => {
                    match response
                        .deframer
                        .next(&mut self.data, &self.turn.assembler.segments)
                    {
                        Ok(Some(message)) => match message.into_plain() {
                            Ok(message) => message,
                            Err(status) => return self.reject(status),
                        },
                        Ok(None) => break,
                        Err(message::Error::Transfer(segments::Error::SegmentCapacity(cause))) => {
                            return self.storage(Resource::Segments, cause);
                        }
                        Err(message::Error::Transfer(segments::Error::ChainCapacity(cause))) => {
                            return self.storage(Resource::Chains, cause);
                        }
                        Err(
                            error @ (message::Error::BadCompressionFlag(_)
                            | message::Error::MessageTooLarge { .. }),
                        ) => {
                            return self.reject(error.into());
                        }
                        Err(status) => {
                            let status = crate::Status::from(status);
                            self.turn.cancel(self.stream);
                            return Err(status);
                        }
                    }
                }
            };
            let held = match Self::publish(self.turn.assembler, self.turn.policy, response, message)
            {
                Ok(Publish::Complete) => None,
                Ok(Publish::Blocked { message, status }) => Some((message, status)),
                Ok(Publish::Rejected(status)) => return self.reject(status),
                Err(status) => {
                    self.turn.cancel(self.stream);
                    return Err(status);
                }
            };
            if let Some((message, status)) = held {
                self.ready = Some(message);
                return self.budget(status);
            }
            if self.data.is_empty() {
                break;
            }
        }
        if matches!(self.boundary, Boundary::End) {
            if !self
                .turn
                .assembler
                .claims
                .try_ready(response.target.mode())?
            {
                return Ok(self.blocked());
            }
            if let Some(status) = http {
                self.turn.finish(self.stream, crate::Status::from(status))?;
                return Ok(turn::Step::Complete);
            }
            let record = self
                .turn
                .egress
                .cancel(self.turn.calls, self.turn.connection, self.stream)
                .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::DataUnknown))?;
            let _ = self
                .turn
                .policy
                .cancel(self.turn.assembler, record.response);
            return Err(crate::Status::new(
                crate::Code::Internal,
                Reason::StatusMissing,
            ));
        }
        Ok(turn::Step::Complete)
    }

    /// The loop admitted this call's delivery slot before rejecting its message.
    fn reject(self, status: crate::Status) -> Result<turn::Step<P, C>, crate::Status> {
        self.turn.finish_reset(self.stream, status)?;
        Ok(turn::Step::Complete)
    }

    /// The loop admitted this call's delivery slot before the capacity failure.
    fn budget(self, status: crate::Status) -> Result<turn::Step<P, C>, crate::Status> {
        if self
            .turn
            .assembler
            .budget
            .can_release(self.turn.calls.charges())
        {
            return Ok(self.blocked());
        }
        self.reject(status)
    }

    /// Transfer shortages precede mutation of this destination pool. Unfinished
    /// responses own disjoint nodes and descriptors; every other occupied slot
    /// belongs to a queued or already polled delivery that consumers can release.
    fn storage(
        self,
        resource: Resource,
        cause: capacity::Shortage,
    ) -> Result<turn::Step<P, C>, crate::Status> {
        let mut used = segments::Plan {
            segments: 0,
            chains: 0,
        };
        for response in self.turn.calls.responses() {
            for count in [
                response.deframer.count(),
                response
                    .message
                    .as_ref()
                    .map_or(segments::Count::EMPTY, |held| held.frame.payload.count()),
            ] {
                used.segments += count.into_raw() as usize;
                used.chains += usize::from(count.is_many());
            }
        }
        let cap = self.turn.assembler.segments.capacity();
        let (room, error) = match resource {
            Resource::Segments => (
                cap.segments - used.segments,
                segments::Error::SegmentCapacity(cause),
            ),
            Resource::Chains => (
                cap.chains - used.chains,
                segments::Error::ChainCapacity(cause),
            ),
        };
        if cause.requested.get() <= room {
            return Ok(self.blocked());
        }
        self.reject(message::Error::Transfer(error).into())
    }

    fn blocked(self) -> turn::Step<P, C> {
        turn::Step::Blocked(turn::Pending::Data {
            stream: self.stream,
            data: self.data,
            boundary: self.boundary,
            ready: self.ready,
        })
    }

    fn publish(
        receive: &mut receive::Assembler<P, C>,
        policy: &mut R,
        response: &mut calls::Response<C>,
        message: message::Frame<C>,
    ) -> Result<Publish<C>, crate::Status> {
        if response.target.mode() == calls::ResponseMode::Unary && response.message.is_some() {
            return Ok(Publish::Rejected(crate::Status::new(
                crate::Code::Unimplemented,
                Reason::UnaryResponseMany,
            )));
        }
        let charge = match receive.budget.admit(message.payload.len()) {
            Ok(Admit::Ready(charge)) => charge,
            Ok(Admit::Blocked(status)) => return Ok(Publish::Blocked { message, status }),
            Err(status) => return Ok(Publish::Rejected(status)),
        };
        match response.target {
            calls::Target::Unary(_) => {
                response.message = Some(calls::Buffered {
                    frame: message,
                    charge,
                });
            }
            calls::Target::Streaming(key) => {
                policy.push_event(
                    receive,
                    key,
                    Ok(receive::Response::Message { message }),
                    Some(charge),
                )?;
            }
        }
        Ok(Publish::Complete)
    }
}
