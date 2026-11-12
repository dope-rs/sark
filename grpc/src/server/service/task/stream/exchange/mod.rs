//! Poll-scoped ingress and egress owned by one streaming fiber.
use std::{cell, mem, task::Poll};

use o3::buffer::resident;

use crate::{
    message, metadata,
    server::service::{
        reply::{Reply, Write},
        task::stream::{Finish, Input, Shape},
    },
};

#[derive(Clone, Copy, Eq, PartialEq)]
enum IngressCardinality {
    Streaming,
    Empty,
    One,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum EgressCardinality {
    Streaming,
    Empty,
    One,
    Excess,
}

#[derive(Default)]
enum Tail {
    #[default]
    Open,
    Ready(metadata::Decoded),
    Seen,
}

pub(super) enum Phase {
    Running,
    Finished(Finish),
    Drained,
}

#[derive(Clone, Copy, Default)]
#[repr(u8)]
pub(in crate::server) enum Events {
    #[default]
    None,
    Input,
    Output,
    Both,
}

pub(in crate::server) struct Exchange<Request, Response, Codec> {
    ingress_cardinality: cell::Cell<IngressCardinality>,
    message: cell::Cell<Option<Request>>,
    tail: cell::Cell<Tail>,
    egress_cardinality: cell::Cell<EgressCardinality>,
    response: cell::Cell<Option<Response>>,
    codec: Codec,
    events: cell::Cell<Events>,
    pub(super) phase: Phase,
}

pub(in crate::server) enum PushError {
    Busy,
    Cardinality,
    Decode(crate::Status),
}

pub(in crate::server) enum CloseError {
    Closed,
    Empty,
}

pub(in crate::server) enum Output {
    Produced,
    Blocked,
    Pending,
    Finish(Finish),
    Excess,
    Missing,
    Error(crate::Status),
}

impl Events {
    const fn input(self) -> Self {
        match self {
            Self::None | Self::Input => Self::Input,
            Self::Output | Self::Both => Self::Both,
        }
    }

    const fn output(self) -> Self {
        match self {
            Self::None | Self::Output => Self::Output,
            Self::Input | Self::Both => Self::Both,
        }
    }
}

impl<Request, Response, Codec> Exchange<Request, Response, Codec> {
    pub(in crate::server) const fn new(codec: Codec, shape: Shape) -> Self {
        Self {
            ingress_cardinality: cell::Cell::new(match shape {
                Shape::Client | Shape::Bidi => IngressCardinality::Streaming,
                Shape::Unary | Shape::Server => IngressCardinality::Empty,
            }),
            message: cell::Cell::new(None),
            tail: cell::Cell::new(Tail::Open),
            egress_cardinality: cell::Cell::new(match shape {
                Shape::Server | Shape::Bidi => EgressCardinality::Streaming,
                Shape::Unary | Shape::Client => EgressCardinality::Empty,
            }),
            response: cell::Cell::new(None),
            codec,
            events: cell::Cell::new(Events::None),
            phase: Phase::Running,
        }
    }

    pub(in crate::server) fn push<'d>(
        &mut self,
        frame: message::Frame<resident::Shared<'d>>,
    ) -> Result<(), PushError>
    where
        Codec: crate::Codec<Owned<resident::Shared<'d>> = Request>,
    {
        if self.message.get_mut().is_some() || !matches!(self.tail.get_mut(), Tail::Open) {
            return Err(PushError::Busy);
        }
        let count = self.ingress_cardinality.get_mut();
        match count {
            IngressCardinality::Streaming | IngressCardinality::Empty => {}
            IngressCardinality::One => return Err(PushError::Cardinality),
        }
        let message = Codec::decode_frame(&mut self.codec, frame).map_err(PushError::Decode)?;
        if *count == IngressCardinality::Empty {
            *count = IngressCardinality::One;
        }
        *self.message.get_mut() = Some(message);
        Ok(())
    }

    pub(in crate::server) fn close(
        &mut self,
        trailers: metadata::Decoded,
    ) -> Result<(), CloseError> {
        let tail = self.tail.get_mut();
        if !matches!(tail, Tail::Open) {
            return Err(CloseError::Closed);
        }
        if self.ingress_cardinality.get() == IngressCardinality::Empty {
            return Err(CloseError::Empty);
        }
        *tail = Tail::Ready(trailers);
        Ok(())
    }

    pub(in crate::server) fn drive<W: Write>(
        &mut self,
        ready: Poll<()>,
        reply: &mut Reply<'_, W>,
    ) -> Output
    where
        Codec: crate::Codec<Encode = Response>,
    {
        let slot = self.response.get_mut();
        let Some(message) = slot.as_ref() else {
            return match mem::replace(&mut self.phase, Phase::Drained) {
                Phase::Finished(finish) => match self.egress_cardinality.get() {
                    EgressCardinality::Excess => Output::Excess,
                    EgressCardinality::Empty if finish.status.code() == crate::Code::Ok => {
                        Output::Missing
                    }
                    EgressCardinality::Streaming
                    | EgressCardinality::Empty
                    | EgressCardinality::One => Output::Finish(finish),
                },
                phase @ (Phase::Running | Phase::Drained) => {
                    self.phase = phase;
                    Output::Pending
                }
            };
        };
        let output = if self.egress_cardinality.get() == EgressCardinality::Excess {
            Output::Excess
        } else if ready.is_pending() {
            return Output::Blocked;
        } else {
            match reply.try_encode(&mut self.codec, message) {
                Ok(Poll::Ready(())) => Output::Produced,
                Ok(Poll::Pending) => return Output::Blocked,
                Err(status) => Output::Error(status),
            }
        };
        *slot = None;
        output
    }

    pub(in crate::server) fn receive(&self) -> Poll<Input<Request>> {
        if let Some(message) = self.message.take() {
            self.events.update(Events::input);
            return Poll::Ready(Input::Message(message));
        }
        match self.tail.take() {
            Tail::Open => Poll::Pending,
            Tail::Ready(trailers) => {
                self.tail.set(Tail::Seen);
                Poll::Ready(Input::Closed(trailers))
            }
            Tail::Seen => {
                self.tail.set(Tail::Seen);
                Poll::Pending
            }
        }
    }

    pub(in crate::server) fn send(&self, message: Response) -> Result<(), Response> {
        if let Some(held) = self.response.take() {
            self.response.set(Some(held));
            return Err(message);
        }
        let next = match self.egress_cardinality.get() {
            EgressCardinality::Streaming => EgressCardinality::Streaming,
            EgressCardinality::Empty => EgressCardinality::One,
            EgressCardinality::One | EgressCardinality::Excess => EgressCardinality::Excess,
        };
        self.response.set(Some(message));
        self.egress_cardinality.set(next);
        self.events.update(Events::output);
        Ok(())
    }

    pub(in crate::server) fn wake(&self) {
        self.events.update(Events::output);
    }

    pub(in crate::server) fn take_events(&self) -> Events {
        self.events.take()
    }
}

const _: () = assert!(core::mem::size_of::<Events>() == core::mem::size_of::<bool>());
