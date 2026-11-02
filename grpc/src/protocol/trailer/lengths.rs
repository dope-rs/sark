use core::mem;

use sark_http::field::text::percent;

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct Lengths {
    message: percent::Length,
    details: u32,
}

impl Lengths {
    pub(crate) const fn new(message: percent::Length, details: u32) -> Self {
        Self { message, details }
    }

    pub(crate) const fn message(self) -> percent::Length {
        self.message
    }

    pub(crate) const fn details(self) -> usize {
        self.details as usize
    }
}

const _: () = assert!(mem::size_of::<Lengths>() == mem::size_of::<u64>());
