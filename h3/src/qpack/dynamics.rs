use dynamic::{hash, insert, mode, plan, table};
use o3::alloc;
use sark_http::payload::compress::{dynamic, dynamic::Field};

use crate::qpack;

pub(super) struct Table<M: mode::Kind = mode::Staged> {
    entries: table::Table<M>,
    core: Core,
}

pub(super) struct Core {
    max_capacity: u64,
    insert_count: u64,
}

pub(super) struct View<'a, M: mode::Kind = mode::Staged> {
    entries: &'a table::Table<M>,
    core: &'a Core,
}

/// Exact failure while changing a bounded dynamic table capacity.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Capacity {
    Maximum { requested: usize, maximum: usize },
    Count { inserted: u64, resident: usize },
    Absolute { oldest: u64, offset: usize },
    Entry { relative: usize, resident: usize },
    Size { name: usize, value: usize },
    Resident { current: usize, entry: usize },
    Protected { requested: usize, minimum: usize },
}

/// Exact dynamic-reference rejection while decoding a field section.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Reference {
    Required {
        absolute: u64,
        required: qpack::Reference,
    },
    PreBase {
        base: u64,
        relative: u64,
    },
    PostBase {
        base: u64,
        relative: u64,
    },
    Future {
        absolute: u64,
        inserted: u64,
    },
    Evicted {
        absolute: u64,
        inserted: u64,
        resident: usize,
    },
}

impl<M: mode::Kind> Clone for View<'_, M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M: mode::Kind> Copy for View<'_, M> {}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Error {
    Reference { relative: u64, len: usize },
    Count { current: u64 },
    Insert(insert::Error),
}

impl From<insert::Error> for Error {
    fn from(error: insert::Error) -> Self {
        Self::Insert(error)
    }
}

impl<M: mode::Kind> Table<M> {
    pub(super) fn from_table(entries: table::Table<M>, max: usize) -> Self {
        Self {
            core: Core {
                max_capacity: max.min(entries.capacity()) as u64,
                insert_count: 0,
            },
            entries,
        }
    }

    pub(super) const fn view(&self) -> View<'_, M> {
        self.core.view(&self.entries)
    }
}

impl Core {
    pub(super) const fn view<'a, M: mode::Kind>(
        &'a self,
        entries: &'a table::Table<M>,
    ) -> View<'a, M> {
        View {
            entries,
            core: self,
        }
    }

    fn maximum(&self, storage: usize) -> usize {
        self.max_capacity.min(storage as u64) as usize
    }

    pub(super) fn check_limit(&self, capacity: usize, storage: usize) -> Result<(), Capacity> {
        let maximum = self.maximum(storage);
        if capacity > maximum {
            return Err(Capacity::Maximum {
                requested: capacity,
                maximum,
            });
        }
        Ok(())
    }

    pub(super) fn next(&self) -> Result<u64, Error> {
        self.insert_count.checked_add(1).ok_or(Error::Count {
            current: self.insert_count,
        })
    }

    pub(super) fn commit(&mut self, next: u64) {
        self.insert_count = next;
    }

    pub(super) fn duplicate(
        &mut self,
        entries: &mut table::Table<mode::Staged>,
        relative: u64,
    ) -> Result<(), Error> {
        let len = entries.entry_count();
        if relative >= len as u64 {
            return Err(Error::Reference { relative, len });
        }
        let next = self.next()?;
        entries.edit().duplicate(relative as usize)?;
        self.commit(next);
        Ok(())
    }
}

impl Table<mode::Direct> {
    pub(super) fn try_new(
        plan: plan::Plan<mode::Direct>,
        max: usize,
        seed: hash::Seed,
    ) -> Result<Self, alloc::Error> {
        Ok(Self::from_table(plan.try_table(max, seed)?, max))
    }

    pub(super) fn allow(&mut self, maximum: u64) {
        self.core.max_capacity = maximum;
    }

    pub(super) fn maximum(&self) -> usize {
        self.core.maximum(self.entries.capacity())
    }

    pub(super) fn parts(&mut self) -> (&mut table::Table<mode::Direct>, &mut u64, u64) {
        (
            &mut self.entries,
            &mut self.core.insert_count,
            self.core.max_capacity,
        )
    }

    pub(super) fn check_floor(&self, capacity: usize, floor: u64) -> Result<(), Capacity> {
        self.core.check_limit(capacity, self.entries.capacity())?;
        let mut size = self.entries.current_size();
        let oldest = self
            .core
            .insert_count
            .checked_sub(self.entries.entry_count() as u64)
            .ok_or(Capacity::Count {
                inserted: self.core.insert_count,
                resident: self.entries.entry_count(),
            })?;
        for offset in 0..self.entries.entry_count() {
            if size <= capacity {
                return Ok(());
            }
            let absolute = oldest
                .checked_add(offset as u64)
                .ok_or(Capacity::Absolute { oldest, offset })?;
            if absolute >= floor {
                return Err(Capacity::Protected {
                    requested: capacity,
                    minimum: size,
                });
            }
            let relative = self.entries.entry_count() - offset - 1;
            let entry = self.entries.field(relative).ok_or(Capacity::Entry {
                relative,
                resident: self.entries.entry_count(),
            })?;
            let entry = entry_size(entry.name().span().len(), entry.value().len()).ok_or(
                Capacity::Size {
                    name: entry.name().span().len(),
                    value: entry.value().len(),
                },
            )?;
            size = size.checked_sub(entry).ok_or(Capacity::Resident {
                current: size,
                entry,
            })?;
        }
        if size > capacity {
            return Err(Capacity::Protected {
                requested: capacity,
                minimum: size,
            });
        }
        Ok(())
    }

    pub(super) fn set_limit(&mut self, capacity: usize) {
        self.entries.edit().set_limit(capacity);
    }
}

impl Table<mode::Staged> {
    pub(super) fn into_parts(self) -> (table::Table<mode::Staged>, Core) {
        (self.entries, self.core)
    }
}

impl<'a, M: mode::Kind> View<'a, M> {
    pub(super) fn capacity(self) -> usize {
        self.entries.max_size()
    }

    pub(super) fn max_entries(self) -> u64 {
        self.core.max_capacity / 32
    }

    pub(super) fn insert_count(self) -> u64 {
        self.core.insert_count
    }

    fn resolve(self, required: qpack::Reference, absolute: u64) -> Result<Field<'a>, Reference> {
        if absolute >= required.required().get() {
            return Err(Reference::Required { absolute, required });
        }
        if absolute >= self.core.insert_count {
            return Err(Reference::Future {
                absolute,
                inserted: self.core.insert_count,
            });
        }
        let relative = self.core.insert_count - absolute - 1;
        let Ok(index) = usize::try_from(relative) else {
            return Err(Reference::Evicted {
                absolute,
                inserted: self.core.insert_count,
                resident: self.entries.entry_count(),
            });
        };
        self.entries.field(index).ok_or(Reference::Evicted {
            absolute,
            inserted: self.core.insert_count,
            resident: self.entries.entry_count(),
        })
    }

    pub(super) fn pre_base(
        self,
        required: qpack::Reference,
        base: u64,
        relative: u64,
    ) -> Result<Field<'a>, Reference> {
        let absolute = base
            .checked_sub(relative)
            .and_then(|value| value.checked_sub(1))
            .ok_or(Reference::PreBase { base, relative })?;
        self.resolve(required, absolute)
    }

    pub(super) fn post_base(
        self,
        required: qpack::Reference,
        base: u64,
        relative: u64,
    ) -> Result<Field<'a>, Reference> {
        let absolute = base
            .checked_add(relative)
            .ok_or(Reference::PostBase { base, relative })?;
        self.resolve(required, absolute)
    }
}

fn entry_size(name_len: usize, value: usize) -> Option<usize> {
    name_len.checked_add(value)?.checked_add(32)
}

const _: () = {
    assert!(usize::BITS <= u64::BITS);
    assert!(core::mem::size_of::<Capacity>() <= 3 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Reference>() <= 4 * core::mem::size_of::<usize>());
};
