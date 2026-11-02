use core::{mem, num};

use plan::{Error, Footprint};

use crate::payload::compress::dynamic::{ENTRY_OVERHEAD, index, mode, plan};

struct Shape {
    capacity: u32,
    ledger: u32,
    bytes: u32,
    entries: u32,
}

#[derive(PartialEq, Eq)]
pub(in crate::payload::compress::dynamic) struct Layout<M: mode::Kind> {
    pub(in crate::payload::compress::dynamic) capacity: u32,
    pub(in crate::payload::compress::dynamic) ledger_len: u32,
    pub(in crate::payload::compress::dynamic) arena_start: u32,
    pub(in crate::payload::compress::dynamic) arena_len: u32,
    pub(in crate::payload::compress::dynamic) period: num::NonZero<u32>,
    pub(in crate::payload::compress::dynamic) index: M::Index,
}

impl<M: mode::Kind> Clone for Layout<M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M: mode::Kind> Copy for Layout<M> {}

impl Shape {
    fn new<M: mode::Kind>(capacity: usize) -> Result<Self, Error> {
        let maximum = u32::MAX as usize;
        if capacity > maximum {
            return Err(Error::Capacity {
                actual: capacity,
                maximum,
            });
        }
        let data = capacity.saturating_sub(ENTRY_OVERHEAD);
        let bytes = data
            .checked_mul(M::ARENAS)
            .filter(|bytes| *bytes <= maximum)
            .ok_or(Error::Bytes {
                data,
                arenas: M::ARENAS,
                maximum,
            })?;
        let entries = capacity / ENTRY_OVERHEAD;
        let width = mem::size_of::<M::Record>();
        let ledger = entries
            .checked_mul(width)
            .filter(|ledger| *ledger <= maximum)
            .ok_or(Error::Ledger {
                entries,
                width,
                maximum,
            })?;
        Ok(Self {
            capacity: capacity as u32,
            ledger: ledger as u32,
            bytes: bytes as u32,
            entries: entries as u32,
        })
    }

    fn finish<M: mode::Kind>(self, index: M::Index, indexed: usize) -> Result<Layout<M>, Error> {
        let maximum = u32::MAX as usize;
        let ledger = self.ledger as usize;
        let prefix = ledger
            .checked_add(indexed)
            .filter(|prefix| *prefix <= maximum)
            .ok_or(Error::Prefix {
                ledger,
                indexed,
                maximum,
            })?;
        let bytes = self.bytes as usize;
        prefix
            .checked_add(bytes)
            .filter(|tail| *tail <= maximum)
            .ok_or(Error::Arena {
                prefix,
                bytes,
                maximum,
            })?;
        Ok(Layout {
            capacity: self.capacity,
            ledger_len: self.ledger,
            arena_start: prefix as u32,
            arena_len: self.bytes,
            period: num::NonZero::<u32>::MIN.saturating_add(self.entries.saturating_sub(1)),
            index,
        })
    }
}

impl Layout<mode::Direct> {
    pub(super) fn new(capacity: usize) -> Result<Self, Error> {
        let shape = Shape::new::<mode::Direct>(capacity)?;
        let (index, indexed) =
            index::State::new(shape.entries as usize, 0).map_err(Error::Index)?;
        shape.finish(index, indexed)
    }
}

impl Layout<mode::Staged> {
    pub(super) fn new(capacity: usize) -> Result<Self, Error> {
        Shape::new::<mode::Staged>(capacity)?.finish((), 0)
    }
}

impl<M: mode::Kind> Layout<M> {
    pub(in crate::payload::compress::dynamic) const fn capacity(&self) -> usize {
        self.capacity as usize
    }

    pub(in crate::payload::compress::dynamic) const fn tail_bytes(&self) -> usize {
        self.arena_start as usize + self.arena_len as usize
    }

    pub(super) const fn footprint(&self, resident: usize) -> Footprint {
        Footprint::new(
            self.arena_len as usize,
            self.arena_start as usize - self.ledger_len as usize,
            self.tail_bytes(),
            resident,
            1,
        )
    }
}

const _: () = {
    assert!(mem::size_of::<Layout<mode::Direct>>() == 44);
    assert!(mem::size_of::<Layout<mode::Staged>>() == 20);
    assert!(mem::align_of::<Layout<mode::Direct>>() == 4);
    assert!(mem::align_of::<Layout<mode::Staged>>() == 4);
};
