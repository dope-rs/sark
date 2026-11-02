use std::{mem::MaybeUninit, slice};

use o3::{alloc::tail, depot::map::compact};

use crate::{
    field::metadata::map::record::Span,
    payload::compress::dynamic::{
        Field, hash,
        index::Key,
        table::{
            delta::buffer::{record, storage::Header},
            state,
        },
    },
};

#[derive(Clone, Copy)]
pub(super) struct View<'a> {
    pub(super) block: tail::View<'a, Header, MaybeUninit<u8>>,
}

pub(super) struct Entry<'a> {
    pub(super) view: View<'a>,
    pub(super) record: &'a record::Record,
}

impl<'a> View<'a> {
    pub(super) fn header(self) -> &'a Header {
        self.block.header()
    }

    pub(super) fn records(self) -> &'a [record::Record] {
        let head = self.header();
        let pointer = self.block.as_slice().as_ptr();
        // SAFETY: the header's alignment also aligns the tail's first record;
        // only the initialized record prefix is published in head.len.
        unsafe { slice::from_raw_parts(pointer.cast(), head.len as usize) }
    }

    pub(super) fn data(self) -> &'a [u8] {
        let head = self.header();
        let pointer = self.block.as_slice().as_ptr().cast::<u8>();
        // SAFETY: the checked tail plan locates the data plane, whose complete
        // initialized prefix is published only after both indices are ready.
        unsafe { slice::from_raw_parts(pointer.add(head.plan.data), head.used as usize) }
    }

    pub(super) fn index(self) -> &'a [u8] {
        let head = self.header();
        let pointer = self.block.as_slice().as_ptr().cast::<u8>();
        // SAFETY: allocation initializes this complete checked index plane.
        unsafe { slice::from_raw_parts(pointer.add(head.plan.index), head.plan.indexed) }
    }

    pub(super) fn get(self, ordinal: usize) -> Option<Entry<'a>> {
        self.records()
            .get(ordinal)
            .map(|record| Entry { view: self, record })
    }
}

impl<'a> Entry<'a> {
    pub(super) fn indexed(value: compact::Value<'_, Lookup<'a>>) -> Self {
        let view = value.owner().view;
        let record = unsafe { view.records().get_unchecked(value.index()) };
        Self { view, record }
    }

    pub(super) fn into_parts(self) -> (Field<'a>, hash::Digest, state::Size) {
        let record = self.record;
        let data = self.view.data();
        // SAFETY: only this view's initialized record prefix creates Entry;
        // admission bounded both ranges before publishing the record and data.
        let name = unsafe { data.get_unchecked(record.start()..record.name_end()) };
        let value = unsafe { data.get_unchecked(record.name_end()..record.end()) };
        (
            Field {
                name: Span::from_slice(name),
                value: Span::from_slice(value),
                proof: record.proof(),
                validation: record.validation(),
            },
            record.digest(),
            state::Size::from_data((record.end() - record.start()) as u32),
        )
    }
}

const _: () = {
    assert!(size_of::<View<'_>>() == size_of::<usize>());
    assert!(size_of::<Entry<'_>>() == 2 * size_of::<usize>());
};

pub(super) struct Lookup<'a> {
    pub(super) view: View<'a>,
    pub(super) key: Key,
}

unsafe impl compact::raw::Read for Lookup<'_> {
    fn layout(&self) -> compact::Layout {
        self.view.header().index.layout(self.key)
    }

    fn bytes(&self) -> &[u8] {
        self.view.index()
    }
}

unsafe impl compact::raw::Source for Lookup<'_> {
    fn hash(value: compact::Value<'_, Self>) -> u64 {
        let record = unsafe { value.owner().view.records().get_unchecked(value.index()) };
        value.owner().key.of(record.digest())
    }
}

unsafe impl<'a> compact::raw::Query for Lookup<'a> {
    type Item = (usize, Field<'a>, usize);

    fn get(value: compact::Value<'_, Self>) -> Self::Item {
        let ordinal = value.index();
        let relative = value.owner().view.header().len as usize - ordinal - 1;
        let (field, _, _) = Entry::indexed(value).into_parts();
        (ordinal, field, relative)
    }
}
