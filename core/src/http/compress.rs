//! ```no_run
//! use sark_core::http::compress::Gzip;
//! let body: &[u8] = b"hello";
//! let zipped = Gzip::new().encode(body).unwrap();
//! ```

use libdeflater::{CompressionLvl, Compressor, DecompressionError, Decompressor};
use o3::buffer::{Bytes, InitializedSharedPool, Pooled, Retained};
use thiserror::Error;

use crate::http::Body;

const GZIP_SLOTS: usize = 32;
const GZIP_CAPACITY: usize = 256 * 1024;

pub struct Gzip {
    encoder: Compressor,
    pool: InitializedSharedPool,
}

impl Gzip {
    const LEVEL: i32 = 3;

    pub fn new() -> Self {
        Self::with_pool(GZIP_SLOTS, GZIP_CAPACITY)
    }

    pub fn with_pool(slots: usize, capacity: usize) -> Self {
        let level = CompressionLvl::new(Self::LEVEL).expect("valid libdeflate compression level");
        Self {
            encoder: Compressor::new(level),
            pool: InitializedSharedPool::new(slots, capacity),
        }
    }

    pub fn encode(&mut self, src: &[u8]) -> Option<Pooled> {
        let cap = self.encoder.gzip_compress_bound(src.len());
        if cap > self.pool.capacity() {
            return None;
        }
        let mut lease = self.pool.try_acquire()?;
        let n = self
            .encoder
            .gzip_compress(src, lease.spare_mut())
            .expect("gzip_compress_bound undersized");
        lease
            .try_advance(n)
            .expect("gzip_compress_bound undersized");
        Some(lease.freeze())
    }
}

impl Default for Gzip {
    fn default() -> Self {
        Self::new()
    }
}

pub enum GunzipOutput {
    Pooled(Pooled),
    Owned(Vec<u8>),
}

impl From<GunzipOutput> for Body<'static> {
    fn from(output: GunzipOutput) -> Self {
        match output {
            GunzipOutput::Pooled(body) => Body::from(Bytes::<Retained>::from(body)),
            GunzipOutput::Owned(body) => Body::from(body),
        }
    }
}

#[derive(Debug, Error)]
pub enum GunzipError {
    #[error("gzip decompression failed: {0}")]
    Invalid(#[from] DecompressionError),
    #[error("decompressed response body exceeds size limit")]
    SizeLimit,
}

pub struct Gunzip {
    decoder: Decompressor,
    pool: InitializedSharedPool,
}

impl Gunzip {
    pub fn new() -> Self {
        Self::with_pool(GZIP_SLOTS, GZIP_CAPACITY)
    }

    pub fn with_pool(slots: usize, capacity: usize) -> Self {
        Self {
            decoder: Decompressor::new(),
            pool: InitializedSharedPool::new(slots, capacity),
        }
    }

    pub fn decode(&mut self, src: &[u8], max_size: usize) -> Result<GunzipOutput, GunzipError> {
        let expected = Self::decoded_size(src)?;
        if expected > max_size {
            return Err(GunzipError::SizeLimit);
        }

        if expected <= self.pool.capacity()
            && let Some(mut lease) = self.pool.try_acquire()
        {
            let written = self
                .decoder
                .gzip_decompress(src, &mut lease.spare_mut()[..expected])?;
            lease
                .try_advance(written)
                .expect("gunzip output exceeded its decoded size");
            return Ok(GunzipOutput::Pooled(lease.freeze()));
        }

        let mut output = vec![0; expected];
        let written = self.decoder.gzip_decompress(src, &mut output)?;
        output.truncate(written);
        Ok(GunzipOutput::Owned(output))
    }

    fn decoded_size(src: &[u8]) -> Result<usize, GunzipError> {
        if src.len() < 18 || !src.starts_with(&[0x1f, 0x8b, 0x08]) {
            return Err(DecompressionError::BadData.into());
        }
        let footer = &src[src.len() - 4..];
        Ok(u32::from_le_bytes(footer.try_into().expect("four-byte gzip footer")) as usize)
    }
}

impl Default for Gunzip {
    fn default() -> Self {
        Self::new()
    }
}
