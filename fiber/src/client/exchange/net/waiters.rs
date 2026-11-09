use std::{marker, pin};

use dope::core::driver::{
    route::Identity,
    schedule::{app, ready::wake},
};
use dope_fiber::context;
use o3::depot::queue::{
    resident::{self, Fixed, fifo},
    tagged::lane,
};
use sark_client::{OriginError, capacity};
use wake::queue;

use crate::client::{dynamic::origin, exchange::net};

pub(in crate::client) struct Waiters<'d, ID: Identity, Tag: net::PoolTag<'d, ID>> {
    active: queue::Queue<'d, Tag::Lanes>,
    connection: Tag::Connections,
    tag: marker::PhantomData<fn(Tag) -> (Tag, ID)>,
}

impl<'d, ID: Identity, Tag: net::PoolTag<'d, ID>> Waiters<'d, ID, Tag> {
    pub(in crate::client) fn try_new(
        active: queue::Queue<'d, Tag::Lanes>,
        connection: Result<Tag::Connections, resident::Error>,
    ) -> Result<Self, resident::Error> {
        Ok(Self {
            active,
            connection: connection?,
            tag: marker::PhantomData,
        })
    }

    pub(in crate::client) fn register<'target>(
        &'target self,
        tag: Tag,
        entry: &mut queue::Registration<'target, 'd, Tag::Lanes>,
        context: pin::Pin<&context::Context<'_, 'd>>,
    ) -> Result<(), capacity::Error> {
        let wake = context.completion();
        tag.register(&self.active, entry, &wake)
            .map_err(capacity::Error::Waiter)
    }

    pub(in crate::client) fn cancel(&self, entry: &mut queue::Registration<'_, 'd, Tag::Lanes>) {
        self.active.cancel(entry);
    }

    pub(in crate::client) fn wake_io(&self) {
        for conn in self.connection.as_ref() {
            conn.wake_one::<0>();
        }
    }

    pub(in crate::client) fn wake(&self, tag: Tag) {
        tag.wake(&self.active);
    }

    pub(in crate::client) fn wake_next(&self) {
        self.active.wake_next();
    }

    pub(in crate::client) fn begin_drain(&self) {
        self.active.begin_drain();
        for conn in self.connection.as_ref() {
            conn.begin_drain();
        }
    }

    pub(in crate::client) fn drain(&self, work: app::Work<'_, 'd>) {
        if self.active.drain_one(work) == wake::Drain::Exhausted {
            return;
        }
        for conn in self.connection.as_ref() {
            conn.drain_one(work);
        }
    }

    pub(in crate::client) fn has_drain(&self) -> bool {
        (self.active.is_draining() && self.active.has_waiters())
            || self
                .connection
                .as_ref()
                .iter()
                .any(|queue| queue.is_draining() && queue.has_waiters())
    }

    pub(in crate::client) fn has_active(&self) -> bool {
        self.active.has_waiters()
    }

    pub(in crate::client) fn has_io(&self) -> bool {
        self.connection
            .as_ref()
            .iter()
            .any(queue::Queue::has_waiters)
    }
}

impl<'d, ID: Identity> Waiters<'d, ID, ()> {
    pub(in crate::client) fn register_io<'target>(
        &'target self,
        entry: &mut queue::Registration<'target, 'd, Fixed<1>>,
        context: pin::Pin<&context::Context<'_, 'd>>,
    ) -> Result<(), fifo::Reject> {
        let [conn] = &self.connection;
        let wake = context.completion();
        conn.try_register::<0>(entry, &wake)
    }
}

impl<'r, 'scope, 'd, ID: Identity> Waiters<'d, ID, origin::Key<'r, 'scope, 'd, ID>> {
    pub(in crate::client) fn try_index(
        &self,
        index: usize,
    ) -> Result<fifo::Index<'scope>, OriginError> {
        self.active.try_index(index).map_err(|error| {
            OriginError::Lane(lane::Error {
                lane: error.index,
                lanes: error.len,
            })
        })
    }
}
