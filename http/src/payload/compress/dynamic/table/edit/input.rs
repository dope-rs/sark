use mode::records;
use o3::{
    depot,
    depot::map::{compact, compact::Insert as _},
};
use table::{edit, state};

use crate::{
    field::metadata::{block, map::record},
    payload::compress::dynamic::{
        ENTRY_OVERHEAD, Field, Whole, field::NAME_LEN_MASK, hash, index::Key, mode, table,
    },
};

pub(in crate::payload::compress::dynamic::table) struct Input<'a, 'field> {
    pub(in crate::payload::compress::dynamic::table) table: &'a mut table::Table<mode::Direct>,
    pub(in crate::payload::compress::dynamic::table) field: Field<'field>,
    pub(in crate::payload::compress::dynamic::table) digest: hash::Digest,
    pub(in crate::payload::compress::dynamic::table) size: state::Size,
}

impl<'a, 'field> Input<'a, 'field> {
    pub(in crate::payload::compress::dynamic::table) fn try_new(
        table: &'a mut table::Table<mode::Direct>,
        name: &'field [u8],
        value: &'field [u8],
    ) -> Result<Self, edit::Error> {
        let size = match Self::try_size(table, name.len(), value.len()) {
            Ok(size) => size,
            Err(error) => {
                table.storage.parts_mut().clear();
                return Err(error);
            }
        };
        let probe = table.probe(record::Entry::new(name, value));
        let proof = block::Name::classify(name);
        Ok(Self {
            digest: probe.digest(),
            field: Field {
                name: record::Span::from_slice(name),
                value: record::Span::from_slice(value),
                proof,
                validation: Whole::from_value(
                    proof,
                    block::Value::from_span(record::Span::from_slice(value)),
                ),
            },
            table,
            size,
        })
    }

    fn try_size(
        table: &table::Table<mode::Direct>,
        name: usize,
        value: usize,
    ) -> Result<state::Size, edit::Error> {
        if name > NAME_LEN_MASK as usize {
            return Err(edit::Error::Name {
                actual: name,
                limit: NAME_LEN_MASK as usize,
            });
        }
        let Some(data) = name.checked_add(value) else {
            return Err(edit::Error::Length { name, value });
        };
        let Some(required) = data.checked_add(ENTRY_OVERHEAD) else {
            return Err(edit::Error::Length { name, value });
        };
        let limit = table.max_size();
        if required > limit {
            return Err(edit::Error::Capacity { required, limit });
        }
        Ok(state::Size::from_data(data as u32))
    }

    pub(in crate::payload::compress::dynamic::table) fn insert(self) {
        let mut next = loop {
            let parts = self.table.storage.parts_mut();
            match state::raw::Ready::admit(parts, self.size) {
                state::Admission::Evict(oldest) => table::Remove::new(oldest).remove(),
                state::Admission::Ready(next) => break next,
            }
        };
        let (slot, tail) = next.encode(self.field);
        let next = next.finish(records::Direct::new(slot, self.digest), tail);
        let exact = table::Insert::<0> { next }.insert();
        exact.insert();
    }
}

pub(in crate::payload::compress::dynamic::table) struct Insert<'a, const N: usize> {
    pub(in crate::payload::compress::dynamic::table) next: state::raw::Push<'a, mode::Direct>,
}

unsafe impl<const N: usize> compact::raw::Read for Insert<'_, N> {
    fn layout(&self) -> compact::Layout {
        self.next
            .parts()
            .state
            .layout
            .index
            .layout(Key::staged::<N>())
    }

    fn bytes(&self) -> &[u8] {
        self.next.parts().index
    }
}

unsafe impl<const N: usize> compact::raw::Source for Insert<'_, N> {
    fn hash(value: compact::Value<'_, Self>) -> u64 {
        let record = unsafe {
            value
                .owner()
                .next
                .parts()
                .ledger
                .get_unchecked(value.index())
        };
        Key::staged::<N>().of(record.digest())
    }
}

unsafe impl<const N: usize> compact::raw::Write for Insert<'_, N> {
    unsafe fn bytes_mut(&mut self) -> &mut [u8] {
        unsafe { self.next.bytes_mut() }
    }
}

unsafe impl<'a> compact::raw::Insert for Insert<'a, 0> {
    type Output = Insert<'a, 1>;

    fn input(&self) -> compact::Value<'_, Self> {
        unsafe { compact::Value::new(depot::raw::Proof::new((self, self.next.raw()))) }
    }

    unsafe fn inserted(self) -> Self::Output {
        Insert { next: self.next }
    }
}

unsafe impl compact::raw::Insert for Insert<'_, 1> {
    type Output = ();

    fn input(&self) -> compact::Value<'_, Self> {
        unsafe { compact::Value::new(depot::raw::Proof::new((self, self.next.raw()))) }
    }

    unsafe fn inserted(self) {
        self.next.commit();
    }
}
