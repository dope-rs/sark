use std::{mem, mem::MaybeUninit, slice};

use o3::alloc::tail;

mod raw;

use crate::payload::compress::dynamic::{
    Field, hash,
    index::{self, Key, Search, State},
    table::{
        delta::buffer::{
            Error,
            append::Append,
            plan::Plan,
            record,
            view::{Entry, Lookup, View},
        },
        state,
    },
};

pub(super) struct Header {
    pub(super) plan: Plan,
    pub(super) index: State,
    pub(super) len: u32,
    pub(super) used: u32,
    pub(super) bytes: u32,
    pub(super) entries: u32,
}

pub struct Storage {
    pub(super) block: tail::Block<Header, MaybeUninit<u8>>,
}

impl Storage {
    pub fn try_new(bytes: usize, entries: usize) -> Result<Self, Error> {
        let maximum = u32::MAX as usize;
        if bytes > maximum {
            return Err(Error::Bytes {
                actual: bytes,
                maximum,
            });
        }
        if entries > maximum {
            return Err(Error::Entries {
                actual: entries,
                maximum,
            });
        }
        let (state, indexed) = match index::State::new(entries, 0) {
            Ok(index) => index,
            Err(cause) => return Err(Error::Index { entries, cause }),
        };
        let width = mem::size_of::<record::Record>();
        let span = match entries.checked_mul(width) {
            Some(len) => len,
            None => return Err(Error::Records { entries, width }),
        };
        let index = span;
        let data = match index.checked_add(indexed) {
            Some(end) => end,
            None => {
                return Err(Error::IndexEnd {
                    start: index,
                    len: indexed,
                });
            }
        };
        let size = match data.checked_add(bytes) {
            Some(end) => end,
            None => {
                return Err(Error::DataEnd {
                    start: data,
                    len: bytes,
                });
            }
        };
        let backing =
            tail::Plan::try_new(size).map_err(|cause| Error::Resident { bytes: size, cause })?;
        let memory = backing.try_allocate().map_err(|cause| Error::Allocation {
            layout: backing.layout(),
            cause: cause.into(),
        })?;
        let block = memory.init(Header {
            plan: Plan {
                index,
                data,
                indexed,
            },
            index: state,
            len: 0,
            used: 0,
            bytes: bytes as u32,
            entries: entries as u32,
        });
        let mut storage = Self { block };
        for (offset, slot) in storage.index_spare().iter_mut().enumerate() {
            slot.write(state.byte(offset));
        }
        Ok(storage)
    }

    pub fn limits(&self) -> (usize, usize) {
        let head = self.header();
        (head.bytes as usize, head.entries as usize)
    }

    pub(in crate::payload::compress::dynamic::table::delta) fn len(&self) -> usize {
        self.header().len as usize
    }

    pub(in crate::payload::compress::dynamic::table::delta) fn append(&mut self) -> Append<'_> {
        Append { storage: self }
    }

    pub(in crate::payload::compress::dynamic::table::delta) fn get(
        &self,
        ordinal: usize,
    ) -> Option<(Field<'_>, state::Size)> {
        self.view().get(ordinal).map(|entry| {
            let (field, _, size) = entry.into_parts();
            (field, size)
        })
    }

    pub(in crate::payload::compress::dynamic::table::delta) fn iter(
        &self,
    ) -> impl DoubleEndedIterator<Item = (Field<'_>, hash::Digest, state::Size)> {
        let view = self.view();
        view.records()
            .iter()
            .map(move |record| Entry { view, record }.into_parts())
    }

    pub(in crate::payload::compress::dynamic::table::delta::buffer) fn find<I: Copy>(
        &self,
        key: Key,
        hash: u64,
        mut accept: impl FnMut(usize, Field<'_>, usize) -> Option<(usize, I)>,
    ) -> Option<I> {
        if self.len() == 0 {
            return None;
        }
        Search::new(Lookup {
            view: self.view(),
            key,
        })
        .find(hash, |(ordinal, field, relative)| {
            accept(ordinal, field, relative)
        })
    }

    /// Count publication follows both indices; zero proves both empty.
    pub(in crate::payload::compress::dynamic::table::delta) fn clear(&mut self) {
        if self.header().len == 0 {
            return;
        }
        // SAFETY: the same exclusive owner retires both memberships before publication.
        let owner = unsafe { index::raw::Clear(self).reset() };
        let head = owner.header_mut();
        head.len = 0;
        head.used = 0;
    }

    pub(super) fn view(&self) -> View<'_> {
        View {
            block: tail::View::new(&self.block),
        }
    }

    pub(super) fn header(&self) -> &Header {
        tail::View::new(&self.block).header()
    }

    pub(super) fn header_mut(&mut self) -> &mut Header {
        self.block.header_mut()
    }

    pub(super) fn index_spare(&mut self) -> &mut [MaybeUninit<u8>] {
        let (head, bytes) = self.block.parts_mut();
        // SAFETY: the immutable plan admitted this exact disjoint index plane
        // before allocating the retained tail. Its offsets never change.
        unsafe { bytes.get_unchecked_mut(head.plan.index..head.plan.data) }
    }

    pub(super) fn index_mut(&mut self) -> &mut [u8] {
        let index = self.index_spare();
        // SAFETY: `try_new` initializes the complete index prefix before publication.
        unsafe { slice::from_raw_parts_mut(index.as_mut_ptr().cast(), index.len()) }
    }
}

const _: () = {
    assert!(!mem::needs_drop::<record::Record>());
    assert!(mem::align_of::<Header>() >= mem::align_of::<record::Record>());
    assert!(core::mem::size_of::<Storage>() == core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Error>() <= 5 * core::mem::size_of::<usize>());
};
