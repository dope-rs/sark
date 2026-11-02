use core::{mem, slice};

use o3::alloc::{self, tail};

use crate::payload::compress::dynamic::{index, mode, plan, table::state};

pub(in crate::payload::compress::dynamic) struct Storage<M: mode::Kind> {
    block: tail::Block<state::State<M>, u8>,
}

pub(in crate::payload::compress::dynamic) struct Memory<'a, M: mode::Kind> {
    block: tail::View<'a, state::State<M>, u8>,
}

pub(in crate::payload::compress::dynamic) struct Parts<'a, M: mode::Kind> {
    pub(in crate::payload::compress::dynamic) state: &'a state::State<M>,
    pub(in crate::payload::compress::dynamic) ledger: &'a [M::Record],
    pub(in crate::payload::compress::dynamic) index: &'a [u8],
    pub(in crate::payload::compress::dynamic) arena: &'a [u8],
}

pub(in crate::payload::compress::dynamic) struct PartsMut<'a, M: mode::Kind> {
    pub(in crate::payload::compress::dynamic) state: &'a mut state::State<M>,
    pub(in crate::payload::compress::dynamic) ledger: &'a mut [M::Record],
    pub(in crate::payload::compress::dynamic) index: &'a mut [u8],
    pub(in crate::payload::compress::dynamic) arena: &'a mut [u8],
}

impl PartsMut<'_, mode::Direct> {
    pub(in crate::payload::compress::dynamic) fn clear(self) {
        // SAFETY: the same exclusive owner retires all records before publication.
        unsafe { index::raw::Clear(self).reset() }.state.clear();
    }
}

impl Storage<mode::Direct> {
    pub(in crate::payload::compress::dynamic) fn try_new(
        plan: plan::Plan<mode::Direct>,
        state: state::State<mode::Direct>,
    ) -> Result<Self, alloc::Error> {
        let memory = plan.allocation.try_allocate()?;
        let block = memory.fill(state, |state, offset| {
            let start = state.layout.ledger_len as usize;
            let end = state.layout.arena_start as usize;
            if offset >= start && offset < end {
                state.layout.index.byte(offset - start)
            } else {
                0
            }
        });
        Ok(Self { block })
    }
}

impl Storage<mode::Staged> {
    pub(in crate::payload::compress::dynamic) fn try_new(
        plan: plan::Plan<mode::Staged>,
        state: state::State<mode::Staged>,
    ) -> Result<Self, alloc::Error> {
        let memory = plan.allocation.try_allocate()?;
        let block = memory.repeat(state, 0);
        Ok(Self { block })
    }
}

impl<M: mode::Kind> Storage<M> {
    pub(in crate::payload::compress::dynamic) fn state(&self) -> &state::State<M> {
        tail::View::new(&self.block).header()
    }

    pub(in crate::payload::compress::dynamic) fn state_mut(&mut self) -> &mut state::State<M> {
        self.block.header_mut()
    }

    pub(in crate::payload::compress::dynamic) fn memory(&self) -> Memory<'_, M> {
        Memory {
            block: tail::View::new(&self.block),
        }
    }

    pub(in crate::payload::compress::dynamic) fn parts_mut(&mut self) -> PartsMut<'_, M> {
        let (state, bytes) = self.block.parts_mut();
        let layout = state.layout;
        let pointer = bytes.as_mut_ptr();
        let records = layout.ledger_len as usize / size_of::<M::Record>();
        let indexed = layout.arena_start as usize - layout.ledger_len as usize;
        let arena = layout.arena_len as usize;
        // SAFETY: `Plan` places the record plane first, validates its exact
        // byte count, and `State<M>` has at least the alignment of both owned
        // record types. Zero initialization and every later write are valid
        // values of the private `M::Record` implementations.
        let ledger = unsafe { slice::from_raw_parts_mut(pointer.cast::<M::Record>(), records) };
        // SAFETY: the index begins after the disjoint record plane and has the
        // exact length encoded by the inseparable allocation layout.
        let index =
            unsafe { slice::from_raw_parts_mut(pointer.add(layout.ledger_len as usize), indexed) };
        // SAFETY: the arena is the final disjoint plane of this allocation.
        let arena =
            unsafe { slice::from_raw_parts_mut(pointer.add(layout.arena_start as usize), arena) };
        PartsMut {
            state,
            ledger,
            index,
            arena,
        }
    }
}

impl<'a, M: mode::Kind> Memory<'a, M> {
    pub(in crate::payload::compress::dynamic) fn state(self) -> &'a state::State<M> {
        self.block.header()
    }

    pub(in crate::payload::compress::dynamic) fn parts(self) -> Parts<'a, M> {
        let state = self.state();
        let layout = state.layout;
        let pointer = self.block.as_slice().as_ptr();
        let records = layout.ledger_len as usize / size_of::<M::Record>();
        let indexed = layout.arena_start as usize - layout.ledger_len as usize;
        let arena = layout.arena_len as usize;
        // SAFETY: `Memory` is created only from the live `Storage` whose plan
        // owns these initialized, aligned record cells for all of `'a`.
        let ledger = unsafe { slice::from_raw_parts(pointer.cast::<M::Record>(), records) };
        // SAFETY: the immutable index plane follows the record plane exactly.
        let index =
            unsafe { slice::from_raw_parts(pointer.add(layout.ledger_len as usize), indexed) };
        // SAFETY: the immutable arena is the allocation's exact final plane.
        let arena =
            unsafe { slice::from_raw_parts(pointer.add(layout.arena_start as usize), arena) };
        Parts {
            state,
            ledger,
            index,
            arena,
        }
    }
}

impl<M: mode::Kind> Clone for Memory<'_, M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M: mode::Kind> Copy for Memory<'_, M> {}

const _: () = {
    assert!(
        mem::align_of::<state::State<mode::Direct>>()
            >= mem::align_of::<<mode::Direct as mode::Private>::Record>()
    );
    assert!(
        mem::align_of::<state::State<mode::Staged>>()
            >= mem::align_of::<<mode::Staged as mode::Private>::Record>()
    );
    assert!(mem::size_of::<Storage<mode::Direct>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Storage<mode::Staged>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Memory<'static, mode::Direct>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Memory<'static, mode::Staged>>() == mem::size_of::<usize>());
};
