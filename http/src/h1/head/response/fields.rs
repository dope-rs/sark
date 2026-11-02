use crate::{
    h1::{field::Connection, framing},
    request::span,
};

#[derive(Clone, Copy, Default)]
pub(super) struct Flags(u8);

impl Flags {
    pub(super) const TIMEOUT: u8 = 1 << 0;
    pub(super) const UPGRADE_AMBIGUOUS: u8 = 1 << 1;

    pub(super) const fn contains(self, flag: u8) -> bool {
        self.0 & flag != 0
    }

    pub(super) fn insert(&mut self, flag: u8) {
        self.0 |= flag;
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct Fields {
    pub(super) coding: super::Coding,
    pub(super) framing: framing::Fields,
    pub(super) connection: Connection,
    pub(super) timeout_secs: u64,
    pub(super) upgrade: Option<span::Span>,
    pub(super) flags: Flags,
}

impl Fields {
    pub(super) const fn timeout(self) -> Option<u64> {
        if self.flags.contains(Flags::TIMEOUT) {
            Some(self.timeout_secs)
        } else {
            None
        }
    }
}

const _: () = assert!(core::mem::size_of::<Fields>() <= 6 * core::mem::size_of::<usize>());
