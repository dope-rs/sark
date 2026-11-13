use std::{mem::MaybeUninit, slice};

use o3::buffer::{
    resident, storage,
    write::{Slice, consume},
};
use sark_http::payload::response::{bounded, exact};

#[repr(C)]
pub(in crate::server::response) struct Inline {
    _align: [usize; 0],
    bytes: [MaybeUninit<u8>; Inline::CAPACITY],
    extent: u8,
    sent: u8,
}

impl Inline {
    pub(in crate::server::response) const CAPACITY: usize =
        size_of::<resident::Shared<'static>>() - size_of::<usize>() - 2 * size_of::<u8>();

    pub(in crate::server::response) fn try_exact<B: exact::TryBody>(
        plan: exact::Plan<B>,
    ) -> Result<Self, storage::BuildError<consume::Error>> {
        let mut bytes = [MaybeUninit::uninit(); Self::CAPACITY];
        let mut out = Slice::uninit(&mut bytes);
        plan.encode(&mut out)?;
        let extent = out.finish() as u8;
        Ok(Self {
            _align: [],
            bytes,
            extent,
            sent: 0,
        })
    }

    pub(in crate::server::response) fn try_bounded<B: bounded::TryBody>(
        plan: bounded::Plan<B>,
    ) -> Result<Self, storage::BuildError<consume::Error>> {
        let mut bytes = [MaybeUninit::uninit(); Self::CAPACITY];
        let mut out = Slice::uninit(&mut bytes);
        plan.encode(&mut out)?;
        let extent = out.finish() as u8;
        Ok(Self {
            _align: [],
            bytes,
            extent,
            sent: 0,
        })
    }

    pub(in crate::server::response) fn as_slice(&self) -> &[u8] {
        // SAFETY: encoding initialized exactly the prefix through extent;
        // admitted transmission advances sent only within that owned prefix.
        unsafe {
            slice::from_raw_parts(
                self.bytes.as_ptr().add(usize::from(self.sent)).cast(),
                self.len(),
            )
        }
    }

    pub(in crate::server::response) fn len(&self) -> usize {
        usize::from(self.extent.wrapping_sub(self.sent))
    }

    pub(in crate::server::response) fn advance(&mut self, written: usize) {
        // The H2 outcome admits written within the exact offered suffix.
        self.sent = self.sent.wrapping_add(written as u8);
    }
}

const _: () = {
    assert!(Inline::CAPACITY > 0);
    assert!(Inline::CAPACITY <= u8::MAX as usize);
    assert!(size_of::<Inline>() + size_of::<usize>() == size_of::<resident::Shared<'static>>());
    assert!(align_of::<Inline>() == align_of::<usize>());
};
