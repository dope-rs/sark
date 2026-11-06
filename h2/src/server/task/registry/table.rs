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

type Entry<'a, 'd, ID, T> =
    slab::Slot<'a, Record<'d, ID, T>, Tag, { u32::MAX }, links::Hook, 0, NonZero<u32>>;

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
        Ok(Vacant::new(task, slot, conn))
    }

    pub(in crate::server::task) fn owner(
        &mut self,
        target: Target<'d, ID>,
        conn: conn::Id<'d, ID>,
    ) -> Option<Entry<'_, 'd, ID, T>> {
        let entry = self.tasks.entry(target.key())?;
        (entry.as_ref().connection == conn).then_some(entry)
    }

    pub(in crate::server::task) fn take_stream(
        &mut self,
        index: &mut Index,
        stream: stream::Key,
    ) -> Option<Taken<'d, ID, T>> {
        let slot = index.entry(stream)?;
        let target = Target::new(*slot.get());
        let entry = self.tasks.entry(target.key())?;
        slot.discard();
        let record = entry.remove();
        Some(Taken {
            target,
            stream: record.stream,
            task: record.task,
            unreported: record.unreported,
        })
    }

    pub(in crate::server::task) fn get(&self, index: &Index, stream: stream::Key) -> Option<&T> {
        Some(&self.tasks.try_get(*index.get(stream)?).ok()?.task)
    }

    pub(in crate::server::task) fn take_target(
        index: &mut Index,
        entry: Entry<'_, 'd, ID, T>,
    ) -> Option<Taken<'d, ID, T>> {
        let target = Target::new(entry.key());
        let record = entry.as_ref();
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
            unreported: record.unreported,
        })
    }
}
