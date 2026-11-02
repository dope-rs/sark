use std::{mem, num::NonZero};

use dope::{core::driver::route::Identity, manifold::network::listener::conn};
use o3::{
    alloc,
    depot::queue::{indexed::slab, links},
};
use task::{
    Target,
    registry::{Error, Index, Record, Tag, Taken, Vacant},
};

use crate::{server::task, stream};

/// Global generation-keyed H2 task storage.
pub(in crate::server::task) struct Table<'d, ID: Identity, T> {
    tasks: slab::Slab<Record<'d, ID, T>, Tag, { u32::MAX }, links::Hook, 0, NonZero<u32>>,
}

impl<'d, ID: Identity + 'd, T> Table<'d, ID, T> {
    pub(in crate::server::task) fn try_new(cap: NonZero<u32>) -> Result<Self, alloc::Error> {
        const {
            assert!(mem::size_of::<Self>() == mem::size_of::<slab::Slab<Record<'d, ID, T>, Tag>>());
            assert!(
                mem::align_of::<Self>() == mem::align_of::<slab::Slab<Record<'d, ID, T>, Tag>>()
            );
        }
        Ok(Self {
            tasks: slab::Slab::try_from(cap)?,
        })
    }

    pub(in crate::server::task) fn vacant<'a>(
        &'a mut self,
        index: &'a mut Index,
        conn: conn::Id<'d, ID>,
        stream: stream::Key,
    ) -> Result<Vacant<'a, 'd, ID, T>, Error> {
        let task = self.tasks.vacant().ok_or(Error::Full)?;
        let slot = index.vacant(stream).ok_or(Error::Occupied)?;
        Ok(Vacant::new(task, slot, conn, stream))
    }

    pub(in crate::server::task) fn owner(
        &mut self,
        target: Target<'d, ID>,
    ) -> Option<(conn::Id<'d, ID>, stream::Key)> {
        let entry = self.tasks.entry(target.key())?;
        let record = entry.as_ref();
        Some((record.connection, record.stream))
    }

    pub(in crate::server::task) fn take_stream(
        &mut self,
        index: &mut Index,
        id: conn::Id<'d, ID>,
        stream: stream::Key,
    ) -> Option<Taken<'d, ID, T>> {
        let slot = index.entry(stream)?;
        let target = Target::new(*slot.get());
        let entry = self.tasks.entry(target.key())?;
        let record = entry.as_ref();
        if record.connection != id || record.stream != stream {
            return None;
        }
        slot.discard();
        let record = entry.remove();
        Some(Taken {
            target,
            stream: record.stream,
            task: record.task,
        })
    }

    pub(in crate::server::task) fn take_target(
        &mut self,
        index: &mut Index,
        target: Target<'d, ID>,
        conn: conn::Id<'d, ID>,
    ) -> Option<Taken<'d, ID, T>> {
        let entry = self.tasks.entry(target.key())?;
        let record = entry.as_ref();
        if record.connection != conn {
            return None;
        }
        let slot = index.entry(record.stream)?;
        if *slot.get() != target.key() {
            return None;
        }
        slot.discard();
        let record = entry.remove();
        Some(Taken {
            target,
            stream: record.stream,
            task: record.task,
        })
    }

    pub(in crate::server::task) fn drain(
        &mut self,
        index: &mut Index,
        conn: conn::Id<'d, ID>,
        mut take: impl FnMut(Taken<'d, ID, T>),
    ) {
        if index.is_empty() {
            return;
        }
        for at in 0..index.capacity() {
            let Some(stream) = index.key_at(at) else {
                continue;
            };
            let Some(task) = self.take_stream(index, conn, stream) else {
                continue;
            };
            take(task);
        }
        index.clear();
    }
}
