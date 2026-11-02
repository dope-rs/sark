use std::{mem, num::NonZero};

use dope::{core::driver::route::Identity, manifold::network::listener::conn};
use o3::depot::{
    map::direct,
    queue::{indexed::slab, links},
};
use slab::phase;
use task::{
    Target,
    registry::{Record, Tag},
};

use crate::{server::task, stream};

type Slot<'a, 'd, ID, T, C = NonZero<u32>> =
    slab::Vacant<'a, Record<'d, ID, T>, Tag, { u32::MAX }, links::Hook, 0, C, phase::Vacant>;

/// Infallible commit capability after both task indices have been validated.
#[must_use = "a vacant H2 task admission is rolled back unless inserted"]
pub(in crate::server::task) struct Vacant<'a, 'd, ID: Identity, T> {
    task: Slot<'a, 'd, ID, T>,
    index: direct::Vacant<'a, stream::Key, slab::Handle<Tag>>,
    connection: conn::Id<'d, ID>,
    stream: stream::Key,
}

impl<'a, 'd, ID: Identity + 'd, T> Vacant<'a, 'd, ID, T> {
    pub(in crate::server::task) fn new(
        task: Slot<'a, 'd, ID, T>,
        index: direct::Vacant<'a, stream::Key, slab::Handle<Tag>>,
        id: conn::Id<'d, ID>,
        stream: stream::Key,
    ) -> Self {
        const {
            assert!(
                mem::size_of::<Slot<'a, 'd, ID, T>>()
                    == mem::size_of::<Slot<'a, 'd, ID, T, o3::depot::capacity::Count>>()
            );
        }
        Self {
            task,
            index,
            connection: id,
            stream,
        }
    }

    pub(in crate::server::task) fn try_insert<E>(
        self,
        build: impl FnOnce(Target<'d, ID>) -> Result<T, E>,
    ) -> Result<(), E> {
        let Self {
            task,
            index,
            connection: id,
            stream,
        } = self;
        let occupied = task.try_insert(|key| {
            let target = Target::new(key);
            build(target).map(|task| Record {
                connection: id,
                stream,
                task,
            })
        })?;
        index.insert(occupied.key());
        Ok(())
    }
}
