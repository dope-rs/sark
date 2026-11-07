use std::num::NonZero;

use o3::buffer::{
    storage,
    write::{self, consume},
};
use offers::{Iter, Owned};

use crate::h1::{
    head::request::{Route, offers, pool},
    switch,
};

#[derive(Clone, Copy)]
pub(super) enum Source<'a> {
    /// A field section the head parser admitted: every line has a colon and CRLF.
    Head(&'a [u8]),
    Compact(&'a [u8]),
}

pub struct View<'a, B = NonZero<u32>> {
    source: Source<'a>,
    bound: B,
}

impl<'a> View<'a> {
    pub fn from_route(route: Route<&'a [u8]>) -> Option<Self> {
        if !matches!(route.transition(), switch::Intent::Upgrade) {
            return None;
        }
        let start = route.line.target.end() + 11;
        let end = route.head_end().get() as usize - 2;
        let source = unsafe { route.bytes().get_unchecked(start..end) };
        Some(Self {
            source: Source::Head(source),
            bound: unsafe { NonZero::new_unchecked(route.offers.compact_len() as u32) },
        })
    }

    pub fn retain(self) -> Result<Owned, storage::BuildError<consume::Error>> {
        let bytes =
            storage::Exact::try_build(self.bound.get() as usize, |writer| self.write_into(writer))?;
        Ok(Owned { bytes })
    }
}

impl<'a> View<'a, ()> {
    pub(in crate::h1::head::request) fn from_meta(meta: &'a pool::Meta<'_>) -> Option<Self> {
        let layout = meta.layout();
        if !matches!(layout.transition(), switch::Intent::Upgrade) {
            return None;
        }
        let data = meta.data();
        let end = layout.head_end();
        let source = if end == 0 {
            Source::Compact(unsafe { data.get_unchecked(layout.target_end()..) })
        } else {
            let start = layout.target_end() + 11;
            Source::Head(unsafe { data.get_unchecked(start..end - 2) })
        };
        Some(Self { source, bound: () })
    }

    pub(super) fn from_owned(owner: &'a Owned) -> Self {
        Self {
            source: Source::Compact(owner.bytes.as_slice()),
            bound: (),
        }
    }
}

impl<'a, B> View<'a, B> {
    pub(in crate::h1) fn values(self) -> Iter<'a> {
        Iter::new(self.source)
    }

    pub(in crate::h1::head::request) fn write_into<W: write::Sink>(
        &self,
        out: &mut W,
    ) -> Result<(), W::Error> {
        if let Source::Compact(bytes) = self.source {
            return out.write_slice(bytes);
        }
        let mut first = true;
        for value in Iter::new(self.source) {
            if !first {
                out.write_byte(0)?;
            }
            out.write_slice(value)?;
            first = false;
        }
        Ok(())
    }
}
