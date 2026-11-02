use crate::{
    field::metadata::{block, map::record::Match},
    payload::compress::dynamic::{ENTRY_OVERHEAD, hash, mode, table, table::state::Size},
};

mod borrow;
pub mod buffer;
mod commit;
mod draft;
mod query;
pub mod raw;

pub use borrow::Borrow;
pub use commit::Commit;
pub use draft::Draft;
pub use query::Query;

/// Location of an entry in a transaction's prospective table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Position {
    /// An entry in the committed table, addressed by its current relative index.
    Base(usize),
    /// An entry in this transaction, addressed by insertion order.
    Staged(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    Name { actual: usize, limit: usize },
    Value { actual: usize, limit: usize },
    Entries { len: usize, limit: usize },
    Bytes { required: usize, limit: usize },
    Length { name: usize, value: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    Add { current: usize, additional: usize },
    Remove { current: usize, removed: usize },
    Counter { position: Position, current: u32 },
    Missing(Position),
    Exhausted { size: usize, limit: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Length { name: usize, value: usize },
    Capacity { required: usize, limit: usize },
    Name { actual: usize, limit: usize },
    Value { actual: usize, limit: usize },
    Entries { len: usize, limit: usize },
    Bytes { required: usize, limit: usize },
    Fault(Fault),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Length { name: usize, value: usize },
    Capacity { required: usize, limit: usize },
    Name { actual: usize, limit: usize },
    Value { actual: usize, limit: usize },
    Entries { len: usize, limit: usize },
    Bytes { required: usize, limit: usize },
    Fault(Fault),
    Eviction(Position),
}

impl From<Admission> for Failure {
    fn from(cause: Admission) -> Self {
        match cause {
            Admission::Name { actual, limit } => Self::Name { actual, limit },
            Admission::Value { actual, limit } => Self::Value { actual, limit },
            Admission::Entries { len, limit } => Self::Entries { len, limit },
            Admission::Bytes { required, limit } => Self::Bytes { required, limit },
            Admission::Length { name, value } => Self::Length { name, value },
        }
    }
}

impl From<Admission> for Error {
    fn from(cause: Admission) -> Self {
        Self::from(Failure::from(cause))
    }
}

impl From<Failure> for Error {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Length { name, value } => Self::Length { name, value },
            Failure::Capacity { required, limit } => Self::Capacity { required, limit },
            Failure::Name { actual, limit } => Self::Name { actual, limit },
            Failure::Value { actual, limit } => Self::Value { actual, limit },
            Failure::Entries { len, limit } => Self::Entries { len, limit },
            Failure::Bytes { required, limit } => Self::Bytes { required, limit },
            Failure::Fault(fault) => Self::Fault(fault),
        }
    }
}

/// A bounded, move-only transaction over a direct dynamic table.
/// Search sees staged entries without mutating `table`; dropping rolls back in O(1).
struct Change {
    evicted: Evictions,
}

#[derive(Clone, Copy)]
struct Evictions {
    size: usize,
    base: u32,
    staged: u32,
}

struct Eviction {
    position: Position,
    next: Evictions,
}

struct Insertion<'a> {
    change: &'a mut Change,
    next: Evictions,
    size: Size,
    proof: block::Name,
}

impl Change {
    fn new(table: &table::Table<mode::Direct>, pending: &mut buffer::Storage) -> Self {
        pending.clear();
        Self {
            evicted: Evictions {
                size: table.current_size(),
                base: 0,
                staged: 0,
            },
        }
    }

    fn both<I: Copy>(
        &self,
        table: &table::Table<mode::Direct>,
        pending: &buffer::Storage,
        probe: hash::Probe<'_>,
        mut map: impl FnMut(Position) -> Option<I>,
    ) -> Match<I> {
        let staged =
            buffer::Search::new(pending).both(probe, self.evicted.staged as usize, |ordinal| {
                map(Position::Staged(ordinal))
            });
        if staged.exact.is_some() {
            return staged;
        }
        let Some(base_len) = self.base_len(table) else {
            return staged;
        };
        let base = table
            .view()
            .search()
            .both(probe, base_len, |relative| map(Position::Base(relative)));
        Match {
            exact: base.exact,
            name: staged.name.or(base.name),
        }
    }

    fn exact<I: Copy>(
        &self,
        table: &table::Table<mode::Direct>,
        pending: &buffer::Storage,
        probe: hash::Probe<'_>,
        mut map: impl FnMut(Position) -> Option<I>,
    ) -> Option<I> {
        let staged =
            buffer::Search::new(pending).exact(probe, self.evicted.staged as usize, |ordinal| {
                map(Position::Staged(ordinal))
            });
        if staged.is_some() {
            return staged;
        }
        let base_len = self.base_len(table)?;
        table
            .view()
            .search()
            .exact(probe, base_len, |relative| map(Position::Base(relative)))
    }

    fn needle<I: Copy>(
        &self,
        table: &table::Table<mode::Direct>,
        pending: &buffer::Storage,
        needle: hash::Needle<'_>,
        mut map: impl FnMut(Position) -> Option<I>,
    ) -> Option<I> {
        let staged =
            buffer::Search::new(pending).needle(needle, self.evicted.staged as usize, |ordinal| {
                map(Position::Staged(ordinal))
            });
        if staged.is_some() {
            return staged;
        }
        let base_len = self.base_len(table)?;
        table
            .view()
            .search()
            .needle(needle, base_len, |relative| map(Position::Base(relative)))
    }

    /// Inserts after evicting the oldest entries required by the table limit.
    fn try_insert<'a>(
        &'a mut self,
        table: &table::Table<mode::Direct>,
        pending: &buffer::Storage,
        probe: hash::Probe<'_>,
    ) -> Result<Insertion<'a>, Failure> {
        let (size, available, proof) = Self::prepare(table, probe)?;
        let next = self.evicted.evict(table, pending, available)?.added(size);
        Ok(Insertion {
            change: self,
            next,
            size,
            proof,
        })
    }

    /// Inserts only when every required eviction is accepted and storage fits.
    fn try_admit<'a>(
        &'a mut self,
        table: &table::Table<mode::Direct>,
        pending: &buffer::Storage,
        probe: hash::Probe<'_>,
        mut accept: impl FnMut(Position) -> bool,
    ) -> Result<Insertion<'a>, Error> {
        let (size, available, proof) = Self::prepare(table, probe)?;
        let next = self
            .evicted
            .try_evict(table, pending, available, &mut accept)?
            .added(size);
        Ok(Insertion {
            change: self,
            next,
            size,
            proof,
        })
    }

    /// Returns the entry's relative index in the prospective table.
    fn relative(&self, pending: &buffer::Storage, position: Position) -> Option<usize> {
        let len = pending.len();
        match position {
            Position::Base(relative) => len
                .checked_sub(self.evicted.staged as usize)?
                .checked_add(relative),
            Position::Staged(ordinal) => len
                .checked_sub(ordinal)
                .and_then(|distance| distance.checked_sub(1)),
        }
    }

    fn base_len(&self, table: &table::Table<mode::Direct>) -> Option<usize> {
        table.entry_count().checked_sub(self.evicted.base as usize)
    }

    fn prepare(
        table: &table::Table<mode::Direct>,
        probe: hash::Probe<'_>,
    ) -> Result<(Size, usize, block::Name), Failure> {
        let field = probe.field();
        let name = field.name.len();
        let value = field.value.len();
        let Some(data) = name.checked_add(value) else {
            return Err(Failure::Length { name, value });
        };
        let Some(size) = data.checked_add(ENTRY_OVERHEAD) else {
            return Err(Failure::Length { name, value });
        };
        let limit = table.max_size();
        let Some(available) = limit.checked_sub(size) else {
            return Err(Failure::Capacity {
                required: size,
                limit,
            });
        };
        let proof = block::Name::classify(field.name);
        Ok((Size::from_data(data as u32), available, proof))
    }
}

impl Insertion<'_> {
    fn commit(self, ready: buffer::Ready<'_, '_>) {
        ready.commit();
        self.change.evicted = self.next;
    }
}

impl Evictions {
    fn evict(
        mut self,
        table: &table::Table<mode::Direct>,
        records: &buffer::Storage,
        available: usize,
    ) -> Result<Self, Failure> {
        while self.size > available {
            let Some(eviction) = self.next(table, records)? else {
                return Err(Failure::Fault(Fault::Exhausted {
                    size: self.size,
                    limit: available,
                }));
            };
            self = eviction.next;
        }
        Ok(self)
    }

    fn try_evict(
        mut self,
        table: &table::Table<mode::Direct>,
        records: &buffer::Storage,
        available: usize,
        accept: &mut impl FnMut(Position) -> bool,
    ) -> Result<Self, Error> {
        while self.size > available {
            let Some(eviction) = self.next(table, records)? else {
                return Err(Error::Fault(Fault::Exhausted {
                    size: self.size,
                    limit: available,
                }));
            };
            let position = eviction.position;
            if !accept(position) {
                return Err(Error::Eviction(position));
            }
            self = eviction.next;
        }
        Ok(self)
    }

    fn next(
        self,
        table: &table::Table<mode::Direct>,
        records: &buffer::Storage,
    ) -> Result<Option<Eviction>, Failure> {
        let base = self.base as usize;
        let count = table.entry_count();
        let (position, size, current) = if base < count {
            let relative = count - base - 1;
            let position = Position::Base(relative);
            let entry = table
                .view()
                .try_entry(relative)
                .ok_or(Fault::Missing(position))
                .map_err(Failure::Fault)?;
            (position, entry.size().get(), self.base)
        } else if (self.staged as usize) < records.len() {
            let ordinal = self.staged as usize;
            let position = Position::Staged(ordinal);
            let size = records
                .get(ordinal)
                .map(|item| item.1.get())
                .ok_or(Fault::Missing(position))
                .map_err(Failure::Fault)?;
            (position, size, self.staged)
        } else {
            return Ok(None);
        };
        let counter = current
            .checked_add(1)
            .ok_or(Fault::Counter { position, current })
            .map_err(Failure::Fault)?;
        let size = self
            .size
            .checked_sub(size)
            .ok_or(Fault::Remove {
                current: self.size,
                removed: size,
            })
            .map_err(Failure::Fault)?;
        let mut next = Self { size, ..self };
        match position {
            Position::Base(_) => next.base = counter,
            Position::Staged(_) => next.staged = counter,
        }
        Ok(Some(Eviction { position, next }))
    }

    fn added(mut self, size: Size) -> Self {
        // Both callers evict to limit - size.get() before adding the admitted size.
        self.size += size.get();
        self
    }
}

const _: () = {
    assert!(core::mem::size_of::<Change>() == 2 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Fault>() == 3 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Failure>() == 3 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Error>() == 3 * core::mem::size_of::<usize>());
};
