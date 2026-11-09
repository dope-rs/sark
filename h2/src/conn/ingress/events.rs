use std::num::NonZero;

use o3::{alloc, buffer::storage, depot::queue::ring};
use sark_core::life::session;

use crate::{
    conn::{self, event},
    frame::numbers,
    stream,
};

type Queue<S, B, C, E> = ring::Fifo<event::Event<S, B, C, E>>;
pub(crate) type Vacant<'a, S, B, C, E> = ring::Vacant<'a, Queue<S, B, C, E>>;

pub(crate) struct Events<
    S,
    B,
    C: session::Storage = storage::Shared,
    E: event::Evidence = event::Retirement,
> {
    pub(in crate::conn) queue: Queue<S, B, C, E>,
}

impl<S, B, C: session::Storage, E: event::Evidence> Events<S, B, C, E> {
    pub(crate) fn try_new(
        capacity: NonZero<u32>,
        first: Option<event::Event<S, B, C, E>>,
    ) -> Result<Self, alloc::Error> {
        let queue = ring::Fifo::try_with(capacity, first)?;
        Ok(Self { queue })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub(crate) fn cancel(&mut self, key: stream::Key) {
        for _ in 0..self.queue.len() {
            let Some(front) = self.queue.front_entry() else {
                break;
            };
            if matches!(
                front.get(),
                event::Event::Headers { stream_id, .. }
                    | event::Event::Data { stream_id, .. }
                    | event::Event::StreamReset { stream_id, .. }
                    if *stream_id == key
            ) {
                front.discard();
            } else {
                let (event, vacant) = front.take();
                vacant.push_back(event);
            }
        }
    }

    pub(crate) fn push(&mut self, event: event::Event<S, B, C, E>) -> Result<(), conn::Error> {
        self.vacant()?.push_back(event);
        Ok(())
    }

    pub(crate) fn vacant(&mut self) -> Result<Vacant<'_, S, B, C, E>, conn::Error> {
        let full = self.full();
        self.queue.vacant().ok_or(full)
    }

    pub(crate) fn try_admit(&self) -> Result<(), conn::Error> {
        if self.queue.is_full() {
            Err(self.full())
        } else {
            Ok(())
        }
    }

    fn full(&self) -> conn::Error {
        conn::Error::EventFull {
            used: self.queue.len(),
            limit: self.queue.capacity(),
        }
    }
}

/// An admitted terminal notification retained until stream cleanup commits.
pub(crate) struct Reset<'a, S, B, C: session::Storage, E: event::Evidence> {
    vacant: Vacant<'a, S, B, C, E>,
    validation: Option<event::Rejected>,
}

impl<'a, S, B, C: session::Storage, E: event::Evidence> Reset<'a, S, B, C, E> {
    pub(crate) fn new(vacant: Vacant<'a, S, B, C, E>, check: Option<event::Rejected>) -> Self {
        Self {
            vacant,
            validation: check,
        }
    }

    pub(crate) fn commit(
        self,
        stream: stream::Key,
        error: numbers::ErrorCode,
        retired: Option<E::Retired>,
    ) {
        self.vacant.push_back(event::Event::StreamReset {
            stream_id: stream,
            error,
            retired,
            validation: self.validation,
        });
    }
}
