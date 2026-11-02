//! Poll-scoped ingress and egress owned by one streaming fiber.
use std::{cell, task::Poll};

use crate::{
    metadata,
    server::service::task::stream::{Finish, Input, Shape},
};

mod egress;
mod ingress;

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
    ingress: ingress::Ingress<Request>,
    egress: egress::Egress<Response>,
    codec: Codec,
    events: cell::Cell<Events>,
}

pub(in crate::server) enum PushError<E> {
    Busy,
    Cardinality,
    Decode(E),
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
            ingress: ingress::Ingress::new(shape),
            egress: egress::Egress::new(shape),
            codec,
            events: cell::Cell::new(Events::None),
        }
    }

    pub(in crate::server) fn push_with<E>(
        &mut self,
        decode: impl FnOnce(&mut Codec) -> Result<Request, E>,
    ) -> Result<(), PushError<E>> {
        self.ingress.push_with(|| decode(&mut self.codec))
    }

    pub(in crate::server) fn close(&mut self, tail: metadata::Decoded) -> Result<(), CloseError> {
        self.ingress.close(tail)
    }

    pub(in crate::server) fn drive(
        &mut self,
        ready: Poll<()>,
        encode: impl FnOnce(&mut Codec, &Response) -> Result<Poll<()>, crate::Status>,
    ) -> Output {
        self.egress
            .drive(ready, |message| encode(&mut self.codec, message))
    }

    pub(in crate::server) fn complete(&mut self, finish: Finish) -> Output {
        self.egress.complete(finish)
    }

    pub(in crate::server) fn receive(&self) -> Poll<Input<Request>> {
        let result = self.ingress.receive();
        if matches!(result, Poll::Ready(Input::Message(_))) {
            self.events.update(Events::input);
        }
        result
    }

    pub(in crate::server) fn send(&self, message: Response) -> Result<(), Response> {
        self.egress.send(message).inspect(|()| {
            self.events.update(Events::output);
        })
    }

    pub(in crate::server) fn wake(&self) {
        self.events.update(Events::output);
    }

    pub(in crate::server) fn take_events(&self) -> Events {
        self.events.take()
    }
}

const _: () = assert!(core::mem::size_of::<Events>() == core::mem::size_of::<bool>());
