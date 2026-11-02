use std::{ops, str};

use o3::buffer::{
    pool,
    pool::{owners, state},
};

use crate::h1::{
    head::request::{offers, pool::Layout},
    switch,
};

/// Request-head authority retained for exactly one upstream response lifetime.
pub struct Meta<'d> {
    pub(in crate::h1::head::request) bytes:
        pool::Span<owners::Plain<pool::Layout>, pool::Borrowed<'d>, state::Uninit>,
}

impl<'d> Meta<'d> {
    pub fn method(&self) -> &str {
        let layout = self.layout();
        self.text(0..layout.method_end())
    }

    pub fn target(&self) -> &str {
        let layout = self.layout();
        self.text(layout.target_start()..layout.target_end())
    }

    pub fn transition(&self) -> switch::Intent {
        self.layout().transition()
    }

    pub fn select(
        &self,
        selected: &[u8],
    ) -> Result<Option<switch::Selection>, switch::UpgradeError> {
        offers::View::from_meta(self)
            .map(|view| switch::Selection::from_offers(view, selected))
            .transpose()
    }

    pub(in crate::h1::head::request) fn layout(&self) -> Layout {
        // SAFETY: both private constructors write a complete native-endian
        // Layout prefix, and Meta never exposes mutable storage.
        unsafe {
            self.bytes
                .as_slice()
                .as_ptr()
                .cast::<Layout>()
                .read_unaligned()
        }
    }

    pub(in crate::h1::head::request) fn data(&self) -> &[u8] {
        // SAFETY: every Meta owns its initialized Layout prefix.
        unsafe { self.bytes.as_slice().get_unchecked(Layout::BYTES..) }
    }

    fn text(&self, range: ops::Range<usize>) -> &str {
        // SAFETY: both private constructors retain the native parser's UTF-8
        // proof in this immutable owner with offsets bounded by its lease.
        unsafe { str::from_utf8_unchecked(self.data().get_unchecked(range)) }
    }
}

const _: () = assert!(size_of::<Meta<'static>>() == 2 * size_of::<usize>());
