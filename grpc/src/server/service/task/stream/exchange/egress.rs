use std::{cell, task::Poll};

use crate::server::service::task::stream::{Finish, Shape, exchange};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Cardinality {
    Streaming,
    Empty,
    One,
    Excess,
}

pub(super) struct Egress<Response> {
    cardinality: cell::Cell<Cardinality>,
    message: cell::Cell<Option<Response>>,
}

impl<Response> Egress<Response> {
    pub(super) const fn new(shape: Shape) -> Self {
        Self {
            cardinality: cell::Cell::new(match shape {
                Shape::Server | Shape::Bidi => Cardinality::Streaming,
                Shape::Unary | Shape::Client => Cardinality::Empty,
            }),
            message: cell::Cell::new(None),
        }
    }

    pub(super) fn send(&self, message: Response) -> Result<(), Response> {
        if let Some(held) = self.message.take() {
            self.message.set(Some(held));
            return Err(message);
        }
        let next = match self.cardinality.get() {
            Cardinality::Streaming => Cardinality::Streaming,
            Cardinality::Empty => Cardinality::One,
            Cardinality::One | Cardinality::Excess => Cardinality::Excess,
        };
        self.message.set(Some(message));
        self.cardinality.set(next);
        Ok(())
    }

    pub(super) fn complete(&self, finish: Finish) -> exchange::Output {
        match self.cardinality.get() {
            Cardinality::Excess => exchange::Output::Excess,
            Cardinality::Empty if finish.status.code() == crate::Code::Ok => {
                exchange::Output::Missing
            }
            Cardinality::Streaming | Cardinality::Empty | Cardinality::One => {
                exchange::Output::Finish(finish)
            }
        }
    }

    pub(super) fn drive(
        &mut self,
        ready: Poll<()>,
        encode: impl FnOnce(&Response) -> Result<Poll<()>, crate::Status>,
    ) -> exchange::Output {
        let slot = self.message.get_mut();
        let Some(message) = slot.as_ref() else {
            return exchange::Output::Pending;
        };
        let output = if self.cardinality.get() == Cardinality::Excess {
            exchange::Output::Excess
        } else if ready.is_pending() {
            return exchange::Output::Blocked;
        } else {
            match encode(message) {
                Ok(Poll::Ready(())) => exchange::Output::Produced,
                Ok(Poll::Pending) => return exchange::Output::Blocked,
                Err(status) => exchange::Output::Error(status),
            }
        };
        *slot = None;
        output
    }
}
