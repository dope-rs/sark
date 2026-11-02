use crate::{
    field::metadata::map::record::Match,
    payload::compress::dynamic::{hash, index, table::delta::buffer},
};

#[derive(Clone, Copy)]
pub(in crate::payload::compress::dynamic::table::delta) struct Search<'a>(&'a buffer::Storage);

impl<'a> Search<'a> {
    pub(in crate::payload::compress::dynamic::table::delta) const fn new(
        storage: &'a buffer::Storage,
    ) -> Self {
        Self(storage)
    }

    pub(in crate::payload::compress::dynamic::table::delta) fn both<I: Copy>(
        self,
        probe: hash::Probe<'_>,
        oldest: usize,
        mut map: impl FnMut(usize) -> Option<I>,
    ) -> Match<I> {
        if let Some(exact) = self.exact(probe, oldest, &mut map) {
            return Match {
                exact: Some(exact),
                name: Some(exact),
            };
        }
        let name = self.needle(probe.needle(), oldest, &mut map);
        Match { exact: None, name }
    }

    pub(in crate::payload::compress::dynamic::table::delta) fn exact<I: Copy>(
        self,
        probe: hash::Probe<'_>,
        oldest: usize,
        mut map: impl FnMut(usize) -> Option<I>,
    ) -> Option<I> {
        let field = probe.field();
        let digest = probe.digest();
        self.0.find(
            index::Key::Exact,
            digest.exact(),
            |ordinal, found, relative| {
                if ordinal >= oldest
                    && found.name.is_equal(field.name)
                    && found.value.is_equal(field.value)
                {
                    map(ordinal).map(|mapped| (relative, mapped))
                } else {
                    None
                }
            },
        )
    }

    pub(in crate::payload::compress::dynamic::table::delta) fn needle<I: Copy>(
        self,
        needle: hash::Needle<'_>,
        oldest: usize,
        mut map: impl FnMut(usize) -> Option<I>,
    ) -> Option<I> {
        self.0.find(
            index::Key::Name,
            needle.hash(),
            |ordinal, field, relative| {
                if ordinal >= oldest && field.name.is_equal(needle.name()) {
                    map(ordinal).map(|mapped| (relative, mapped))
                } else {
                    None
                }
            },
        )
    }
}
