pub(super) mod layout;
mod overflow;

use o3::alloc::{self, tail};
pub use overflow::Overflow;
use table::{Storage, state::State};

use crate::payload::compress::dynamic::{hash, index, insert, mode, table};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    Capacity {
        actual: usize,
        maximum: usize,
    },
    Bytes {
        data: usize,
        arenas: usize,
        maximum: usize,
    },
    Ledger {
        entries: usize,
        width: usize,
        maximum: usize,
    },
    Index(index::Error),
    Prefix {
        ledger: usize,
        indexed: usize,
        maximum: usize,
    },
    Arena {
        prefix: usize,
        bytes: usize,
        maximum: usize,
    },
    Resident {
        bytes: usize,
        cause: alloc::Error,
    },
    Footprint(Overflow),
}

/// Exact requested heap footprint of one or more dynamic tables.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Footprint {
    arena: usize,
    index: usize,
    tail: usize,
    resident: usize,
    allocations: usize,
}

impl Footprint {
    pub(super) const fn new(
        arena: usize,
        index: usize,
        tail: usize,
        resident: usize,
        count: usize,
    ) -> Self {
        Self {
            arena,
            index,
            tail,
            resident,
            allocations: count,
        }
    }

    pub const fn arena_bytes(self) -> usize {
        self.arena
    }

    pub const fn index_bytes(self) -> usize {
        self.index
    }

    pub const fn ledger_bytes(self) -> usize {
        self.tail - self.arena - self.index
    }

    pub const fn tail_bytes(self) -> usize {
        self.tail
    }

    pub const fn resident(self) -> usize {
        self.resident
    }

    pub const fn allocations(self) -> usize {
        self.allocations
    }

    pub fn checked_add(self, other: Self) -> Result<Self, Error> {
        let arena =
            self.arena
                .checked_add(other.arena)
                .ok_or(Error::Footprint(Overflow::Arena {
                    left: self.arena,
                    right: other.arena,
                }))?;
        let index =
            self.index
                .checked_add(other.index)
                .ok_or(Error::Footprint(Overflow::Index {
                    left: self.index,
                    right: other.index,
                }))?;
        let tail = self
            .tail
            .checked_add(other.tail)
            .ok_or(Error::Footprint(Overflow::Tail {
                left: self.tail,
                right: other.tail,
            }))?;
        let resident = self
            .resident
            .checked_add(other.resident)
            .ok_or(Error::Footprint(Overflow::Resident {
                left: self.resident,
                right: other.resident,
            }))?;
        let count = self
            .allocations
            .checked_add(other.allocations)
            .ok_or(Error::Footprint(Overflow::Allocations {
                left: self.allocations,
                right: other.allocations,
            }))?;
        Ok(Self::new(arena, index, tail, resident, count))
    }
}

#[derive(Copy, Clone)]
pub struct Plan<M: mode::Kind = mode::Staged> {
    pub(super) layout: layout::Layout<M>,
    pub(super) allocation: tail::Plan<State<M>, u8>,
}

impl<M: mode::Kind> Plan<M> {
    fn from(layout: layout::Layout<M>) -> Result<Self, Error> {
        let bytes = layout.tail_bytes();
        let tail = match tail::Plan::try_new(bytes) {
            Ok(tail) => tail,
            Err(alloc::Overflow) => {
                return Err(Error::Resident {
                    bytes,
                    cause: alloc::Error::Overflow,
                });
            }
        };
        Ok(Self {
            layout,
            allocation: tail,
        })
    }

    pub const fn capacity(self) -> usize {
        self.layout.capacity()
    }

    pub const fn footprint(self) -> Footprint {
        self.layout.footprint(self.allocation.size())
    }

    pub const fn tail_bytes(self) -> usize {
        self.layout.tail_bytes()
    }
}

impl Plan<mode::Direct> {
    pub fn new(capacity: usize) -> Result<Self, Error> {
        Self::from(layout::Layout::<mode::Direct>::new(capacity)?)
    }

    pub fn try_table(
        self,
        limit: usize,
        seed: hash::Seed,
    ) -> Result<table::Table<mode::Direct>, alloc::Error> {
        let state = State::new(self.layout, limit, seed, ());
        let storage = Storage::<mode::Direct>::try_new(self, state)?;
        Ok(table::Table { storage })
    }
}

impl Plan<mode::Staged> {
    pub fn new(capacity: usize) -> Result<Self, Error> {
        Self::from(layout::Layout::<mode::Staged>::new(capacity)?)
    }

    pub fn try_table(self, limit: usize) -> Result<table::Table<mode::Staged>, alloc::Error> {
        let state = State::new(self.layout, limit, (), insert::State::default());
        let storage = Storage::<mode::Staged>::try_new(self, state)?;
        Ok(table::Table { storage })
    }
}

#[derive(Clone, Copy)]
pub struct Plans {
    encoder: Plan<mode::Direct>,
    decoder: Plan<mode::Staged>,
    footprint: Footprint,
}

impl Plans {
    pub fn new(encoder: usize, decoder: usize) -> Result<Self, Error> {
        let encoder = Plan::<mode::Direct>::new(encoder)?;
        let decoder = Plan::<mode::Staged>::new(decoder)?;
        Self::try_from((encoder, decoder))
    }

    pub const fn encoder(self) -> Plan<mode::Direct> {
        self.encoder
    }

    pub const fn decoder(self) -> Plan<mode::Staged> {
        self.decoder
    }

    pub const fn footprint(self) -> Footprint {
        self.footprint
    }
}

impl TryFrom<(Plan<mode::Direct>, Plan<mode::Staged>)> for Plans {
    type Error = Error;

    fn try_from(
        (encoder, decoder): (Plan<mode::Direct>, Plan<mode::Staged>),
    ) -> Result<Self, Error> {
        let total = encoder.footprint().checked_add(decoder.footprint())?;
        Ok(Self {
            encoder,
            decoder,
            footprint: total,
        })
    }
}

const _: () = assert!(core::mem::size_of::<Footprint>() == 5 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Error>() == 4 * core::mem::size_of::<usize>());
