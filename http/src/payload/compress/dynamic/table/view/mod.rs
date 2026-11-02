use mode::Private as _;
use o3::depot::map::compact;
use table::ring;

use crate::payload::compress::dynamic::{
    Field,
    index::Key,
    mode, table,
    table::{
        Memory, Parts,
        ring::{Cursor, Ring},
        slot, state,
    },
};

pub(super) mod entry;

pub struct View<'a, M: mode::Kind> {
    memory: Memory<'a, M>,
}

impl<'a, M: mode::Kind> View<'a, M> {
    pub(super) const fn new(memory: Memory<'a, M>) -> Self {
        Self { memory }
    }

    pub(in crate::payload::compress::dynamic::table) fn try_entry(
        self,
        index: usize,
    ) -> Option<entry::Entry<'a, M>> {
        let parts = self.memory.parts();
        let physical = self.physical(index)?;
        let record = parts.ledger.get(physical)?;
        let slot = M::slot(record);
        let size = self.size(&parts, index, slot)?;
        let data_len = size.data_len();
        let name_len = slot.name_len();
        if name_len > data_len {
            return None;
        }
        let ring = Ring::new(parts.arena);
        let name = ring.stored(slot.start(), name_len);
        let start = ring.advance(slot.start(), name_len);
        let value = ring.stored(start, data_len - name_len);
        let field = Field {
            name,
            value,
            proof: slot.proof(),
            validation: slot.validation(),
        };
        Some(entry::Entry::new(field, record, size))
    }

    fn physical(self, index: usize) -> Option<usize> {
        let state = self.state();
        state.entries.index(state.layout.period).physical(index)
    }

    fn size(self, parts: &Parts<'_, M>, index: usize, slot: slot::Slot) -> Option<state::Size> {
        let end = if index == 0 {
            self.state().entries.tail()
        } else {
            let physical = self.physical(index - 1)?;
            M::slot(parts.ledger.get(physical)?).start()
        };
        let ring = Ring::new(parts.arena);
        if (ring.len() == 0 && (slot.start() != Cursor::ZERO || end != Cursor::ZERO))
            || (ring.len() != 0
                && (slot.start().index() >= ring.len() || end.index() >= ring.len()))
        {
            return None;
        }
        let distance = slot.start().distance(end, self.state().layout.arena_len);
        if distance != 0 || !self.state().entries.is_single() {
            return Some(state::Size::from_data(distance));
        }
        Some(state::Size::from_total(self.state().entries.size()))
    }

    pub(super) fn state(self) -> &'a state::State<M> {
        self.memory.state()
    }
}

impl<'a> View<'a, mode::Direct> {
    pub fn search(self) -> table::Search<'a> {
        table::Search::new(self)
    }

    pub(super) fn lookup(self, key: Key) -> Lookup<'a> {
        Lookup::new(self, key)
    }
}

impl View<'_, mode::Staged> {
    pub(in crate::payload::compress::dynamic) fn slot(self, index: usize) -> Option<slot::Slot> {
        let physical = self.physical(index)?;
        Some(mode::Staged::slot(
            self.memory.parts().ledger.get(physical)?,
        ))
    }
}

impl<M: mode::Kind> Clone for View<'_, M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M: mode::Kind> Copy for View<'_, M> {}

const _: () = {
    assert!(core::mem::size_of::<View<'static, mode::Direct>>() == core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<View<'static, mode::Staged>>() == core::mem::size_of::<usize>());
};

pub(in crate::payload::compress::dynamic::table) struct Lookup<'a> {
    view: table::View<'a, mode::Direct>,
    key: Key,
}

impl<'a> Lookup<'a> {
    pub(in crate::payload::compress::dynamic::table::view) const fn new(
        view: table::View<'a, mode::Direct>,
        key: Key,
    ) -> Self {
        Self { view, key }
    }
}

unsafe impl compact::raw::Read for Lookup<'_> {
    fn layout(&self) -> compact::Layout {
        self.view.state().layout.index.layout(self.key)
    }

    fn bytes(&self) -> &[u8] {
        self.view.memory.parts().index
    }
}

unsafe impl compact::raw::Source for Lookup<'_> {
    fn hash(value: compact::Value<'_, Self>) -> u64 {
        let parts = value.owner().view.memory.parts();
        let record = unsafe { parts.ledger.get_unchecked(value.index()) };
        value.owner().key.of(record.digest())
    }
}

unsafe impl<'a> compact::raw::Query for Lookup<'a> {
    type Item = (usize, Field<'a>);

    fn get(value: compact::Value<'_, Self>) -> Self::Item {
        let owner = value.owner().view;
        let state = owner.state();
        let parts = owner.memory.parts();
        let physical = value.index();
        let count = value.capacity();
        let relative =
            state::position(count, physical as u32, count.get() - state.entries.front()) as usize;
        let record = unsafe { parts.ledger.get_unchecked(physical) };
        let slot = mode::Direct::slot(record);
        let end = if relative == 0 {
            state.entries.tail()
        } else {
            let newer = state::position(count, physical as u32, count.get() - 1) as usize;
            mode::Direct::slot(unsafe { parts.ledger.get_unchecked(newer) }).start()
        };
        let distance = slot.start().distance(end, state.layout.arena_len);
        let size = if distance == 0 && state.entries.is_single() {
            state::Size::from_total(state.entries.size())
        } else {
            state::Size::from_data(distance)
        };
        let ring = ring::Ring::new(parts.arena);
        let name_len = slot.name_len();
        let name = ring.stored(slot.start(), name_len);
        let start = ring.advance(slot.start(), name_len);
        let field = Field {
            name,
            value: ring.stored(start, size.data_len() - name_len),
            proof: slot.proof(),
            validation: slot.validation(),
        };
        (relative, field)
    }
}
