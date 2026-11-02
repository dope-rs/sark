use o3::depot::index::set;
use sark_h2::stream::Key;

use crate::server::config;

#[derive(Clone, Copy)]
pub struct Entry<'a> {
    key: Key,
    row: set::Entry<&'a set::State<config::Call, usize, 3>, config::Call, usize, 3>,
}

impl<'a> Entry<'a> {
    pub(in crate::server) const fn key(self) -> Key {
        self.key
    }

    pub(in crate::server::runtime::state::active) const fn new(
        key: Key,
        row: set::Entry<&'a set::State<config::Call, usize, 3>, config::Call, usize, 3>,
    ) -> Self {
        Self { key, row }
    }

    pub(in crate::server) fn app(self) {
        self.row.set(set::Id::at::<0>());
    }

    pub(in crate::server) fn output(self) {
        self.row.clear(set::Id::at::<2>());
        self.row.set(set::Id::at::<1>());
    }

    pub(in crate::server) fn block(self) {
        self.row.clear(set::Id::at::<1>());
        self.row.set(set::Id::at::<2>());
    }

    pub(in crate::server) fn cancel(self) {
        self.row.clear(set::Id::at::<0>());
        self.row.clear(set::Id::at::<1>());
        self.row.clear(set::Id::at::<2>());
    }
}

const _: () = {
    assert!(size_of::<Entry<'static>>() == 3 * size_of::<usize>());
    assert!(size_of::<Option<Entry<'static>>>() == size_of::<Entry<'static>>());
};
