use core::{mem, ops};

use crate::field::base64::{Error, decode::View};

/// Mutable storage used for an overlapping decode.
///
#[repr(transparent)]
pub struct Decoder<'a>(&'a mut [u8]);

impl<'a> Decoder<'a> {
    pub fn new(bytes: &'a mut [u8]) -> Self {
        Self(bytes)
    }

    /// Decodes an overlapping range whose output remains inside the owner.
    pub fn decode(&mut self, source: ops::Range<usize>, dst: usize) -> Result<usize, Error> {
        Ok(View::try_new(&mut *self.0, source, dst)?.commit())
    }
}

const _: () = assert!(mem::size_of::<Decoder<'_>>() == mem::size_of::<&mut [u8]>());
