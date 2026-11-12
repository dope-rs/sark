use std::{cell, marker, ops};

use o3::{alloc, depot::capacity};

use crate::manifold::{
    self,
    schedule::{
        Activation, Block, CONNECTION_WAIT, CONTROL_CONNECTION_WAIT, CONTROL_EGRESS_WAIT,
        CONTROL_READY, CONTROL_STREAM_WAIT, DATA_READY, Driver, Egress, InsertResult, Occupied,
        Records, Report, STREAM_WAIT, Terminal, Turn, Vacant, Wait, Wake, data, drive::Drive,
        policy::Policy,
    },
};

/// Bounded scheduling driver over one canonical resident queue domain.
pub struct Runtime<K: manifold::raw::Key, T, B: Block> {
    records: Records<K, T>,
    policy: Policy,
    block: marker::PhantomData<cell::Cell<B>>,
}

impl<K: manifold::raw::Key, T, B: Block> Runtime<K, T, B> {
    pub fn try_bounded(cap: capacity::Count, limit: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self {
            records: Records::try_new(cap, limit)?,
            policy: Policy::default(),
            block: marker::PhantomData,
        })
    }

    pub fn has_runnable(&self) -> bool {
        !self.records.slots.is_vacant(CONTROL_READY)
            || (!self.policy.is_blocked() && !self.records.slots.is_vacant(DATA_READY))
    }

    /// Admits one resident owner and retains its exact queue membership.
    pub fn entry(&mut self, stream: K) -> Option<Occupied<'_, K, T, B>> {
        Some(Occupied::new(
            self.records.slots.entry(stream)?,
            &mut self.policy,
        ))
    }

    pub fn vacant(&mut self, stream: K) -> Option<Vacant<'_, K, T, B>> {
        if self.records.len() == self.records.limit.get() {
            return None;
        }
        self.records.slots.vacant(stream).map(Vacant::new)
    }

    pub fn try_insert(&mut self, stream: K, value: T, next: Activation<B>) -> InsertResult<K, T> {
        let (vacant, value) = self.records.try_vacant(stream, value)?;
        let entry = vacant.insert(value);
        Occupied::new(entry, &mut self.policy).activate(next);
        Ok(())
    }

    /// Constructs an idle record only after admitting its exact storage position.
    pub fn try_store(&mut self, stream: K, make: impl FnOnce() -> T) -> InsertResult<K, ()> {
        self.records.try_vacant(stream, ())?.0.insert(make());
        Ok(())
    }

    pub fn wake(&mut self, wake: Wake<K>) -> usize {
        match wake {
            Wake::Connection => {
                self.records
                    .slots
                    .move_all(CONTROL_CONNECTION_WAIT, CONTROL_READY)
                    + self.records.slots.move_all(CONNECTION_WAIT, DATA_READY)
            }
            Wake::Stream(stream) => {
                let entry = match self.records.slots.entry(stream) {
                    Some(entry) => entry,
                    None => return 0,
                };
                let next = match entry.lane() {
                    Some(lane) if lane == CONTROL_STREAM_WAIT => CONTROL_READY,
                    Some(lane) if lane == STREAM_WAIT => DATA_READY,
                    _ => return 0,
                };
                entry.move_to_back(next);
                1
            }
        }
    }

    pub fn wake_egress(&mut self) {
        self.records
            .slots
            .move_all(CONTROL_EGRESS_WAIT, CONTROL_READY);
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
        self.records.slots.clear();
        self.policy.reset();
    }

    pub fn drive<D>(&mut self, driver: &mut D) -> Report
    where
        D: Driver<T, Key = K, Block = B>,
    {
        Drive::new(&mut self.records.slots, &mut self.policy, driver).run::<false>()
    }

    pub fn drive_data<D>(&mut self, driver: &mut D) -> Report
    where
        D: data::Driver<T, Key = K, Block = B>,
    {
        let mut report = Report::default();
        let mut left = self.records.len();
        while left != 0 && !self.policy.is_blocked() {
            let Some(mut current) = self.records.slots.front(DATA_READY) else {
                break;
            };
            if !driver.can_continue() {
                break;
            }
            left -= 1;
            let stream = current.key();
            let turn = driver.turn(stream, current.get_mut());
            let (terminal, lane, blocked) = match turn {
                Ok(Turn::Complete) => (Some(Terminal::Complete), None, false),
                Ok(Turn::Idle) => (Some(Terminal::Idle), None, false),
                Ok(Turn::Progress) => (None, Some(DATA_READY), false),
                Ok(Turn::Blocked(block)) => match block.wait() {
                    Wait::Connection => (None, Some(CONNECTION_WAIT), false),
                    Wait::Stream => (None, Some(STREAM_WAIT), false),
                    Wait::Egress => (None, None, true),
                },
                Err(error) => (Some(Terminal::Failed(error)), None, false),
            };
            let complete = matches!(&terminal, Some(Terminal::Complete));
            match terminal.map(|terminal| driver.settle(stream, current.get_mut(), terminal)) {
                Some(data::Settlement::Keep) => current.unlink(),
                Some(data::Settlement::Remove) => {
                    let value = current.remove();
                    driver.remove(value);
                }
                None => {
                    if let Some(lane) = lane {
                        current.move_to_back(lane);
                    }
                }
            }
            report.turns += 1;
            report.completed += usize::from(complete);
            if blocked {
                self.policy.block_data();
            }
        }
        report
    }
}

impl<K: manifold::raw::Key, T, B: Block> ops::Deref for Runtime<K, T, B> {
    type Target = Records<K, T>;

    fn deref(&self) -> &Self::Target {
        &self.records
    }
}

impl<K: manifold::raw::Key, T, B: Block> ops::DerefMut for Runtime<K, T, B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.records
    }
}
