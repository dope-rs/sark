use o3::{
    alloc, buffer,
    depot::{
        arena::{slab, slab::key},
        capacity,
    },
};
use receive::budget;
use state::calls;

use crate::client::{receive, state};

pub(super) struct Delivery<T> {
    pub(super) value: Result<T, crate::Status>,
    pub(super) charge: Option<budget::Charge>,
}

enum Completion {
    Pending,
    Ready(key::Handle<ReadyTag>),
}

enum ReadyTag {}

enum Stream {
    Empty,
    Open {
        head: key::Handle<EventTag>,
        tail: Tail,
    },
}

#[derive(Clone, Copy)]
enum Tail {
    Open(key::Handle<EventTag>),
    Closed,
}

#[derive(Debug)]
pub(crate) enum EventTag {}

const _: () = assert!(
    core::mem::size_of::<Stream>() <= 2 * core::mem::size_of::<Option<key::Handle<EventTag>>>()
);

struct Event<C: buffer::raw::Segment> {
    delivery: Delivery<receive::Response<C>>,
    next: Option<key::Handle<EventTag>>,
}

#[derive(Debug)]
pub(crate) enum Error {
    Completion(key::Error<receive::UnaryTag>),
    Duplicate(usize),
    Ready(usize),
    Stream(key::Error<receive::StreamTag>),
    Closed {
        key: key::Handle<receive::StreamTag>,
    },
    Event(key::Error<EventTag>),
    Link(key::Error<EventTag>),
    UnaryCapacity(slab::Vacancy),
    StreamCapacity(slab::Vacancy),
    ReadyCapacity(slab::Vacancy),
    EventCapacity(slab::Vacancy),
}

pub(super) enum UnaryCancel {
    Missing,
    Complete(Option<budget::Charge>),
}

pub(super) struct Rejected {
    error: Error,
    charge: Option<budget::Charge>,
}

impl Rejected {
    fn new(error: Error, charge: Option<budget::Charge>) -> Self {
        Self { error, charge }
    }

    pub(super) fn status(
        self,
        fault: &mut receive::Fault,
        budget: &mut budget::Budget,
    ) -> crate::Status {
        fault.release(budget, self.charge);
        match fault.take() {
            Ok(()) => crate::Status::claim(self.error),
            Err(error) => error,
        }
    }
}

pub(super) struct Claims<C: buffer::raw::Segment> {
    completions: slab::Store<Completion, receive::UnaryTag>,
    ready: slab::Store<Delivery<receive::UnaryResult<C>>, ReadyTag>,
    streams: slab::Store<Stream, receive::StreamTag>,
    events: slab::Store<Event<C>, EventTag>,
    pub(super) offset: usize,
}

impl<C: buffer::raw::Segment> Claims<C> {
    pub(super) fn try_new(
        unary: capacity::Count,
        ready: capacity::Count,
        streams: capacity::Count,
        events: capacity::Count,
        offset: usize,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            completions: slab::Store::<_, receive::UnaryTag>::try_new(unary)?,
            ready: slab::Store::<_, ReadyTag>::try_new(ready)?,
            streams: slab::Store::<_, receive::StreamTag>::try_new(streams)?,
            events: slab::Store::<_, EventTag>::try_new(events)?,
            offset,
        })
    }

    pub(super) fn poll_unary(
        &mut self,
        key: receive::UnaryKey,
    ) -> Result<receive::Progress<Delivery<receive::UnaryResult<C>>>, crate::Status> {
        let claim = self
            .completions
            .try_entry(key.0)
            .map_err(|cause| crate::Status::claim(Error::Completion(cause)))?;
        let ready = match claim.get() {
            Completion::Pending => return Ok(receive::Progress::Pending(key.slot())),
            Completion::Ready(ready) => *ready,
        };
        let ready = self
            .ready
            .remove(ready)
            .ok_or_else(|| crate::Status::claim(Error::Ready(ready.index() as usize)))?;
        claim.remove();
        Ok(receive::Progress::Ready(ready))
    }

    pub(super) fn claim_unary(&mut self) -> Result<receive::UnaryKey, crate::Status> {
        let slot = self
            .completions
            .try_vacant()
            .map_err(|cause| crate::Status::claim(Error::UnaryCapacity(cause)))?;
        Ok(receive::UnaryKey(slot.insert(Completion::Pending)))
    }

    pub(super) fn cancel_unary(&mut self, key: receive::UnaryKey) -> Result<UnaryCancel, Error> {
        let Some(claim) = self.completions.remove(key.0) else {
            return Ok(UnaryCancel::Missing);
        };
        match claim {
            Completion::Pending => Ok(UnaryCancel::Complete(None)),
            Completion::Ready(ready) => {
                let ready = self
                    .ready
                    .remove(ready)
                    .ok_or(Error::Ready(ready.index() as usize))?;
                Ok(UnaryCancel::Complete(ready.charge))
            }
        }
    }

    pub(super) fn claim_stream(&mut self) -> Result<receive::StreamKey, crate::Status> {
        let slot = self
            .streams
            .try_vacant()
            .map_err(|cause| crate::Status::claim(Error::StreamCapacity(cause)))?;
        Ok(receive::StreamKey(slot.insert(Stream::Empty)))
    }

    pub(super) fn poll_stream(
        &mut self,
        key: receive::StreamKey,
    ) -> Result<receive::Progress<Delivery<receive::Response<C>>>, crate::Status> {
        let Self {
            streams,
            events,
            offset,
            ..
        } = self;
        let mut stream = streams
            .try_entry(key.0)
            .map_err(|cause| crate::Status::claim(Error::Stream(cause)))?;
        let (head, tail) = match stream.get() {
            Stream::Empty => return Ok(receive::Progress::Pending(key.slot(*offset))),
            Stream::Open { head, tail } => (*head, *tail),
        };
        let node = events
            .try_entry(head)
            .map_err(|cause| crate::Status::claim(Error::Event(cause)))?
            .remove();
        match (node.next, tail) {
            (Some(head), tail) => *stream.get_mut() = Stream::Open { head, tail },
            (None, Tail::Open(_)) => *stream.get_mut() = Stream::Empty,
            (None, Tail::Closed) => {
                stream.remove();
            }
        }
        Ok(receive::Progress::Ready(node.delivery))
    }

    pub(super) fn cancel_feed(
        &mut self,
        key: receive::StreamKey,
        mut release: impl FnMut(budget::Charge),
    ) -> Result<receive::Cancel, Error> {
        let slot = key.slot(self.offset);
        let Some(stream) = self.streams.remove(key.0) else {
            return Ok(receive::Cancel::Missing);
        };
        let mut next = match stream {
            Stream::Empty => None,
            Stream::Open { head, .. } => Some(head),
        };
        while let Some(key) = next {
            let node = self.events.try_entry(key).map_err(Error::Event)?.remove();
            next = node.next;
            if let Some(charge) = node.delivery.charge {
                release(charge);
            }
        }
        Ok(receive::Cancel::Complete(slot))
    }

    pub(super) fn try_ready(&self, mode: calls::ResponseMode) -> Result<bool, crate::Status> {
        let result = match mode {
            calls::ResponseMode::Unary => self.ready.try_ready(),
            calls::ResponseMode::Streaming => self.events.try_ready(),
        };
        match result {
            Ok(()) => Ok(true),
            Err(slab::Vacancy::Full) => Ok(false),
            Err(cause @ slab::Vacancy::IdentityExhausted) => {
                Err(crate::Status::claim(match mode {
                    calls::ResponseMode::Unary => Error::ReadyCapacity(cause),
                    calls::ResponseMode::Streaming => Error::EventCapacity(cause),
                }))
            }
        }
    }

    pub(super) fn push_unary(
        &mut self,
        key: receive::UnaryKey,
        value: Result<receive::UnaryResult<C>, crate::Status>,
        charge: Option<budget::Charge>,
    ) -> Result<(), Rejected> {
        let claims = &mut self.completions;
        let ready = &mut self.ready;
        let claim = match claims.try_entry(key.0) {
            Ok(claim) => claim.into_mut(),
            Err(cause) => return Err(Rejected::new(Error::Completion(cause), charge)),
        };
        if matches!(claim, Completion::Ready(_)) {
            return Err(Rejected::new(
                Error::Duplicate(key.0.index() as usize),
                charge,
            ));
        }
        let slot = match ready.try_vacant() {
            Ok(slot) => slot,
            Err(cause) => return Err(Rejected::new(Error::ReadyCapacity(cause), charge)),
        };
        let ready = slot.insert(Delivery { value, charge });
        *claim = Completion::Ready(ready);
        Ok(())
    }

    pub(super) fn push_event(
        &mut self,
        key: receive::StreamKey,
        value: Result<receive::Response<C>, crate::Status>,
        charge: Option<budget::Charge>,
    ) -> Result<(), Rejected> {
        let Self {
            streams, events, ..
        } = self;
        let stream = match streams.try_entry(key.0) {
            Ok(stream) => stream.into_mut(),
            Err(cause) => return Err(Rejected::new(Error::Stream(cause), charge)),
        };
        let tail = match &*stream {
            Stream::Empty => None,
            Stream::Open {
                tail: Tail::Open(tail),
                ..
            } => Some(*tail),
            Stream::Open {
                tail: Tail::Closed, ..
            } => return Err(Rejected::new(Error::Closed { key: key.0 }, charge)),
        };
        let slot = match events.try_vacant() {
            Ok(slot) => slot,
            Err(cause) => return Err(Rejected::new(Error::EventCapacity(cause), charge)),
        };
        let node = Event {
            delivery: Delivery { value, charge },
            next: None,
        };
        let closed = match &node.delivery.value {
            Err(_) | Ok(receive::Response::Trailers { .. }) => true,
            Ok(receive::Response::Headers { .. } | receive::Response::Message { .. }) => false,
        };
        let ready = if let Some(tail) = tail {
            let (slot, mut previous) = match slot.try_split(tail) {
                Ok(entries) => entries,
                Err(cause) => return Err(Rejected::new(Error::Link(cause), node.delivery.charge)),
            };
            let ready = slot.insert(node);
            previous.get_mut().next = Some(ready);
            ready
        } else {
            slot.insert(node)
        };
        let terminal = if closed {
            Tail::Closed
        } else {
            Tail::Open(ready)
        };
        match stream {
            stream @ Stream::Empty => {
                *stream = Stream::Open {
                    head: ready,
                    tail: terminal,
                };
            }
            Stream::Open { tail, .. } => *tail = terminal,
        }
        Ok(())
    }
}
