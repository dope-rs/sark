//! In-place gzip framing around the raw deflate encoder.

use std::{mem, num};

use compress::Gzip;
use miniz_oxide::deflate::core::{
    CompressorOxide, TDEFLFlush, TDEFLStatus, compress, create_comp_flags_from_zip_params,
};
use o3::{
    alloc,
    buffer::{self, pool},
};

use crate::payload::{compress, crc32::Hasher};

/// Fixed gzip member framing: a 10-byte header and an 8-byte trailer.
const HEADER: usize = 10;
const TRAILER: usize = 8;
pub(super) const FRAMING: usize = HEADER + TRAILER;

/// Compression level handed to deflate within its closed 1..=10 range.
const LEVEL: i32 = 3;

/// One reusable raw-deflate encoder and an empty admitted checksum prototype.
pub(super) struct Encoder(Box<CompressorOxide>, Hasher);

impl Encoder {
    /// Builds a raw-deflate encoder: this module writes the gzip framing, so the
    /// encoder must not emit a wrapper header of its own.
    pub(super) fn try_new() -> Result<Self, alloc::Error> {
        let flags = create_comp_flags_from_zip_params(LEVEL, 0, 0);
        Ok(Self(
            alloc::BoxExt::try_new(CompressorOxide::new(flags))?,
            Hasher::new(),
        ))
    }

    /// Gzip extent bounded by zlib's `deflateBound`, including framing and
    /// stored-block overhead for the input length.
    pub(super) const fn bound(&self, len: usize) -> usize {
        HEADER + len + (len >> 12) + (len >> 14) + (len >> 25) + 13 + TRAILER
    }

    /// Encodes one gzip member, resetting the encoder so no previous stream's
    /// dictionary state leaks in. A refused or partial stream reports
    /// `None` so the caller keeps its payload.
    pub(super) fn encode<'a>(
        &mut self,
        src: &[u8],
        mut output: buffer::raw::Output<
            'a,
            pool::Fixed<{ Gzip::SLOTS.get() }, { Gzip::CAPACITY.get() }>,
        >,
    ) -> Option<pool::Complete<'a, pool::Fixed<{ Gzip::SLOTS.get() }, { Gzip::CAPACITY.get() }>>>
    {
        let capacity = output.len();
        let bytes = output.as_mut_slice();
        let room = capacity.checked_sub(HEADER + TRAILER)?;
        self.0.reset();
        let (status, read, wrote) = compress(
            &mut self.0,
            src,
            &mut bytes[HEADER..HEADER + room],
            TDEFLFlush::Finish,
        );
        if status != TDEFLStatus::Done || read != src.len() {
            return None;
        }
        let written = wrote
            .checked_add(HEADER + TRAILER)
            .filter(|written| *written <= capacity && *written < src.len())
            .and_then(num::NonZero::new)?;
        bytes[..HEADER].copy_from_slice(&[0x1f, 0x8b, 0x08, 0, 0, 0, 0, 0, 0, 0xff]);
        let end = written.get() - TRAILER;
        let mut checksum = self.1.clone();
        checksum.update(src);
        bytes[end..end + 4].copy_from_slice(&checksum.finalize().to_le_bytes());
        // SAFETY: written bounds this unchanged output and includes the eight-byte trailer.
        unsafe { bytes.get_unchecked_mut(end + 4..written.get()) }
            .copy_from_slice(&(src.len() as u32).to_le_bytes());
        // SAFETY: deflate reported a complete stream and the exact nonzero extent
        // was bounded above while this output stayed borrowed by its owner.
        Some(unsafe { output.complete(written) })
    }
}

const _: () = assert!(mem::size_of::<Encoder>() == 2 * mem::size_of::<usize>());
