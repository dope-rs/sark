use crate::{
    field::metadata::map::record,
    payload::compress::dynamic::{hash, index, mode, table::View},
};

#[derive(Clone, Copy)]
pub struct Search<'a> {
    view: View<'a, mode::Direct>,
}

impl<'a> Search<'a> {
    pub(super) const fn new(view: View<'a, mode::Direct>) -> Self {
        Self { view }
    }

    pub fn get(self, name: &[u8], value: &[u8]) -> record::Match<usize> {
        let state = self.view.state();
        self.both(
            state.hash.probe(record::Entry::new(name, value)),
            state.entries.len(),
            Some,
        )
    }

    pub(super) fn both<I: Copy>(
        self,
        probe: hash::Probe<'_>,
        retained: usize,
        mut map: impl FnMut(usize) -> Option<I>,
    ) -> record::Match<I> {
        if let Some(exact) = self.find_exact(probe, retained, &mut map) {
            return record::Match {
                exact: Some(exact),
                name: Some(exact),
            };
        }
        let name = self.find_needle(probe.needle(), retained, &mut map);
        record::Match { exact: None, name }
    }

    pub(super) fn exact<I: Copy>(
        self,
        probe: hash::Probe<'_>,
        retained: usize,
        mut map: impl FnMut(usize) -> Option<I>,
    ) -> Option<I> {
        self.find_exact(probe, retained, &mut map)
    }

    pub(super) fn needle<I: Copy>(
        self,
        needle: hash::Needle<'_>,
        retained: usize,
        mut map: impl FnMut(usize) -> Option<I>,
    ) -> Option<I> {
        self.find_needle(needle, retained, &mut map)
    }

    fn find_exact<I: Copy>(
        self,
        probe: hash::Probe<'_>,
        retained: usize,
        map: &mut impl FnMut(usize) -> Option<I>,
    ) -> Option<I> {
        let field = probe.field();
        let digest = probe.digest();
        index::Search::new(self.view.lookup(index::Key::Exact)).find(
            digest.exact(),
            |(relative, entry)| {
                if relative < retained
                    && entry.name.is_equal(field.name)
                    && entry.value.is_equal(field.value)
                {
                    map(relative).map(|mapped| (relative, mapped))
                } else {
                    None
                }
            },
        )
    }

    fn find_needle<I: Copy>(
        self,
        needle: hash::Needle<'_>,
        retained: usize,
        map: &mut impl FnMut(usize) -> Option<I>,
    ) -> Option<I> {
        index::Search::new(self.view.lookup(index::Key::Name)).find(
            needle.hash(),
            |(relative, entry)| {
                if relative < retained && entry.name.is_equal(needle.name()) {
                    map(relative).map(|mapped| (relative, mapped))
                } else {
                    None
                }
            },
        )
    }
}
