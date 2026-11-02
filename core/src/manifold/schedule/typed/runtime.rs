use std::{cell, marker, ops};

use o3::{
    alloc,
    depot::{capacity, queue::indexed::lanes::typed},
};

use crate::manifold::{
    self,
    schedule::{
        ALL, Block, CONNECTION_WAIT, CONTROL_CONNECTION_WAIT, CONTROL_EGRESS_WAIT, CONTROL_READY,
        CONTROL_STREAM_WAIT, DATA_READY, Egress, Insert, InsertResult, LANES, Report, STREAM_WAIT,
        Wake,
        drive::Drive,
        policy::Policy,
        typed::{Driver, Entry, Next, Vacant, source},
    },
};

/// One bounded domain whose payload phase owns its scheduling class.
pub struct Runtime<K: manifold::raw::Key, C, D, I, B: Block> {
    slots: typed::Table<K, C, D, I, LANES>,
    limit: capacity::Count,
    policy: Policy,
    block: marker::PhantomData<cell::Cell<B>>,
}

impl<K: manifold::raw::Key, C, D, I, B: Block> Runtime<K, C, D, I, B> {
    pub fn try_bounded(cap: capacity::Count, limit: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: typed::Table::try_new(cap)?,
            limit: limit.min(cap),
            policy: Policy::default(),
            block: marker::PhantomData,
        })
    }

    pub fn entry(&mut self, stream: K) -> Option<Entry<'_, K, C, D, I, B>> {
        Some(Entry::new(self.slots.entry(stream)?, &mut self.policy))
    }

    pub fn remove(&mut self, stream: K) -> Option<typed::Value<C, D, I>> {
        self.entry(stream).map(Entry::remove)
    }

    pub fn vacant(&mut self, stream: K) -> Option<Vacant<'_, K, C, D, I, B>> {
        if self.slots.len() == self.limit.get() {
            return None;
        }
        Some(Vacant::new(self.slots.vacant(stream)?, &mut self.policy))
    }

    pub fn try_insert(
        &mut self,
        stream: K,
        next: Next<C, D, I, B>,
    ) -> InsertResult<K, Next<C, D, I, B>> {
        let used = self.slots.len();
        let limit = self.limit.get();
        let slot = match self.slots.try_vacant(stream) {
            Ok(slot) => slot,
            Err(error) => return Err(Insert::from_direct(error, next)),
        };
        if used == limit {
            return Err(Insert::Full {
                stream,
                value: next,
                used,
                limit,
            });
        }
        Vacant::new(slot, &mut self.policy).insert(next);
        Ok(())
    }

    pub fn has_work(&self) -> bool {
        ALL.iter().any(|&lane| !self.slots.is_vacant(lane))
    }

    pub fn has_runnable(&self) -> bool {
        !self.slots.is_vacant(CONTROL_READY)
            || (!self.policy.is_blocked() && !self.slots.is_vacant(DATA_READY))
    }

    pub fn wake(&mut self, wake: Wake<K>) -> usize {
        match wake {
            Wake::Connection => {
                self.slots.move_all(CONTROL_CONNECTION_WAIT, CONTROL_READY)
                    + self.slots.move_all(CONNECTION_WAIT, DATA_READY)
            }
            Wake::Stream(stream) => match self.slots.entry(stream) {
                Some(typed::Entry::Control(entry)) if entry.lane() == Some(CONTROL_STREAM_WAIT) => {
                    entry.move_to_back(CONTROL_READY);
                    1
                }
                Some(typed::Entry::Data(entry)) if entry.lane() == Some(STREAM_WAIT) => {
                    entry.move_to_back(DATA_READY);
                    1
                }
                _ => 0,
            },
        }
    }

    pub fn wake_egress(&mut self) {
        self.slots.move_all(CONTROL_EGRESS_WAIT, CONTROL_READY);
        self.policy.release_data();
    }

    pub fn resume(&mut self, mut poll: impl FnMut() -> Option<Wake<K>>, egress: Egress) {
        while let Some(wake) = poll() {
            self.wake(wake);
        }
        match egress {
            Egress::Unchanged => {}
            Egress::Released => self.wake_egress(),
        }
    }

    pub fn clear(&mut self) {
        self.slots.clear();
        self.policy.reset();
    }

    pub fn drive<F>(&mut self, driver: &mut F) -> Report
    where
        F: Driver<C, D, I, Key = K, Block = B>,
    {
        Drive::with(
            source::Source::new(&mut self.slots, driver),
            &mut self.policy,
        )
        .run::<false>()
    }

    pub fn drive_round<F>(&mut self, driver: &mut F) -> Report
    where
        F: Driver<C, D, I, Key = K, Block = B>,
    {
        Drive::with(
            source::Source::new(&mut self.slots, driver),
            &mut self.policy,
        )
        .run::<true>()
    }
}

impl<K: manifold::raw::Key, C, D, I, B: Block> ops::Deref for Runtime<K, C, D, I, B> {
    type Target = typed::Table<K, C, D, I, LANES>;

    fn deref(&self) -> &Self::Target {
        &self.slots
    }
}
