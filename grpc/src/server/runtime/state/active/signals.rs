use o3::{
    alloc,
    depot::{
        index,
        index::{bitmap::Fill, set},
    },
};
use sark_core::manifold;
use sark_h2::stream::Key;

use crate::server::{config, runtime::state::active};

/// One connection's readiness storage and checked row authority.
pub(in crate::server) struct Signals {
    sets: set::State<config::Call, usize, 3>,
}

impl Signals {
    pub(in crate::server) fn try_new(
        domain: index::Domain<config::Call>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            sets: set::State::try_new(domain, [Fill::Empty; 3])?,
        })
    }

    pub(in crate::server) const fn capacity(&self) -> usize {
        self.sets.capacity()
    }

    pub(in crate::server) fn try_entry(
        &self,
        key: Key,
    ) -> Result<active::Entry<'_>, active::Error> {
        Ok(active::Entry::new(
            key,
            self.sets
                .try_entry(index::Ordinal::new(manifold::raw::Key::slot(key)))
                .map_err(|cause| active::Error { key, cause })?,
        ))
    }

    pub(in crate::server) fn is_quiescent(&self) -> bool {
        !self.has_work() && self.sets.get::<2>().is_empty()
    }

    pub(in crate::server::runtime) fn clear(&self) {
        if !self.is_quiescent() {
            self.sets.clear();
        }
    }

    pub(in crate::server) fn has_work(&self) -> bool {
        self.has_semantic() || self.has_output()
    }

    pub(in crate::server) fn has_semantic(&self) -> bool {
        !self.sets.get::<0>().is_empty()
    }

    pub(in crate::server::runtime) fn blocked(
        &self,
        cursor: usize,
        rows: usize,
    ) -> Option<set::Entry<&set::State<config::Call, usize, 3>, config::Call, usize, 3>> {
        let plane = self.sets.get::<2>();
        if rows == 0 || plane.is_empty() {
            return None;
        }
        let tail = self.capacity() - cursor;
        if let Some(entry) = plane.find(index::Ordinal::new(cursor))
            && entry.index().raw() - cursor < rows.min(tail)
        {
            return Some(entry);
        }
        if rows <= tail {
            return None;
        }
        plane
            .find(index::Ordinal::new(0))
            .filter(|entry| entry.index().raw() < rows - tail)
    }

    pub(in crate::server) fn has_output(&self) -> bool {
        !self.sets.get::<1>().is_empty()
    }

    pub(in crate::server::runtime) fn pop<const PLANE: usize>(
        &self,
        cursor: usize,
    ) -> Option<set::Entry<&set::State<config::Call, usize, 3>, config::Call, usize, 3>> {
        self.sets
            .get::<PLANE>()
            .pop_from(index::Ordinal::new(cursor))
            .map(set::Entry::into_entry)
    }
}
