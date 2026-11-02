use table::{
    ring::{Buf, Cursor},
    slot::Slot,
    state,
};

use crate::payload::compress::dynamic::{ENTRY_OVERHEAD, Field, mode, table};

pub(in crate::payload::compress::dynamic::table) struct Push<'a, M: mode::Kind> {
    parts: table::PartsMut<'a, M>,
    physical: u32,
    len: u32,
    tail: Cursor,
    size: u32,
}

pub(in crate::payload::compress::dynamic::table) struct Ready<'a, M: mode::Kind> {
    parts: table::PartsMut<'a, M>,
    physical: u32,
    len: u32,
    size: u32,
}

pub(in crate::payload::compress::dynamic::table) struct Pop<'a, M: mode::Kind> {
    parts: table::PartsMut<'a, M>,
    physical: u32,
    len: u32,
    size: u32,
}

impl<'a, M: mode::Kind> Push<'a, M> {
    pub(in crate::payload::compress::dynamic::table) const fn raw(&self) -> u32 {
        self.physical
    }

    pub(in crate::payload::compress::dynamic::table) fn parts(&self) -> &table::PartsMut<'a, M> {
        &self.parts
    }

    pub(in crate::payload::compress::dynamic::table) unsafe fn bytes_mut(&mut self) -> &mut [u8] {
        self.parts.index
    }

    pub(in crate::payload::compress::dynamic::table) fn commit(self) {
        let entries = &mut self.parts.state.entries;
        entries.front = self.physical;
        entries.len = self.len;
        entries.tail = self.tail;
        entries.size = self.size;
    }
}

impl<'a, M: mode::Kind> Ready<'a, M> {
    pub(in crate::payload::compress::dynamic::table) fn admit(
        parts: table::PartsMut<'a, M>,
        add: state::Size,
    ) -> state::Admission<'a, M> {
        let state = &parts.state;
        let entries = &state.entries;
        let count = state.layout.period.get();
        if entries.len != 0
            && (entries.size as u64 + add.get() as u64 > u64::from(entries.limit)
                || entries.len >= count)
        {
            return state::Admission::Evict(Pop::last(parts));
        }
        let physical = if entries.len == 0 {
            0
        } else if entries.front == 0 {
            count - 1
        } else {
            entries.front - 1
        };
        let len = entries.len + 1;
        let size = entries.size + ENTRY_OVERHEAD as u32 + add.0;
        state::Admission::Ready(Self {
            parts,
            physical,
            len,
            size,
        })
    }
    pub(in crate::payload::compress::dynamic::table) fn encode(
        &mut self,
        field: Field<'_>,
    ) -> (Slot, Cursor) {
        let start = self.parts.state.entries.tail();
        let slot = Slot::pack(start, field.name.len(), field.validation);
        let mut ring = Buf::new(self.parts.arena);
        let (first, second) = field.name.parts();
        let cursor = ring.write(start, first);
        let cursor = ring.write(cursor, second);
        let (first, second) = field.value.parts();
        let cursor = ring.write(cursor, first);
        let tail = ring.write(cursor, second);
        (slot, tail)
    }

    pub(in crate::payload::compress::dynamic::table) fn finish(
        self,
        record: M::Record,
        tail: Cursor,
    ) -> Push<'a, M> {
        let parts = self.parts;
        unsafe {
            parts
                .ledger
                .as_mut_ptr()
                .add(self.physical as usize)
                .write(record)
        };
        Push {
            parts,
            physical: self.physical,
            len: self.len,
            tail,
            size: self.size,
        }
    }
}

impl<'a, M: mode::Kind> Pop<'a, M> {
    pub(in crate::payload::compress::dynamic::table) fn oldest(
        parts: table::PartsMut<'a, M>,
    ) -> Option<Self> {
        let entries = &parts.state.entries;
        if entries.len == 0 || entries.size <= entries.limit {
            return None;
        }
        Some(Self::last(parts))
    }

    fn last(parts: table::PartsMut<'a, M>) -> Self {
        let entries = &parts.state.entries;
        let count = parts.state.layout.period;
        let len = entries.len - 1;
        let physical = state::position(count, entries.front, len);
        let start = M::slot(unsafe { parts.ledger.get_unchecked(physical as usize) }).start();
        let end = if len == 0 {
            entries.tail
        } else {
            let newer = state::position(count, entries.front, len - 1) as usize;
            M::slot(unsafe { parts.ledger.get_unchecked(newer) }).start()
        };
        let distance = start.distance(end, parts.state.layout.arena_len);
        let size = if distance == 0 && len == 0 {
            0
        } else {
            entries.size - ENTRY_OVERHEAD as u32 - distance
        };
        Self {
            parts,
            physical,
            len,
            size,
        }
    }

    pub(in crate::payload::compress::dynamic::table) const fn raw(&self) -> u32 {
        self.physical
    }

    pub(in crate::payload::compress::dynamic::table) fn parts(&self) -> &table::PartsMut<'a, M> {
        &self.parts
    }

    pub(in crate::payload::compress::dynamic::table) unsafe fn bytes_mut(&mut self) -> &mut [u8] {
        self.parts.index
    }
}

impl Pop<'_, mode::Direct> {
    pub(in crate::payload::compress::dynamic::table) fn commit(self) {
        let entries = &mut self.parts.state.entries;
        entries.size = self.size;
        entries.len = self.len;
        entries.reset();
    }
}

impl<'a> Pop<'a, mode::Staged> {
    pub(in crate::payload::compress::dynamic::table) fn commit(
        self,
    ) -> table::PartsMut<'a, mode::Staged> {
        let entries = &mut self.parts.state.entries;
        entries.size = self.size;
        entries.len = self.len;
        entries.reset();
        self.parts
    }
}

const _: () = {
    assert!(size_of::<Push<'_, mode::Direct>>() == 9 * size_of::<usize>());
    assert!(size_of::<Pop<'_, mode::Direct>>() == 9 * size_of::<usize>());
    assert!(size_of::<Ready<'_, mode::Direct>>() == 9 * size_of::<usize>());
};
