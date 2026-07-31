//! ```no_run
//! use sark_core::http::compress::Gzip;
//! let body: &[u8] = b"hello";
//! let zipped = Gzip::new().encode(body).unwrap();
//! ```

use libdeflater::{CompressionLvl, Compressor, DecompressionError, Decompressor};
use o3::buffer::{
    Bytes, Initialized, PoolLayoutError, Pooled, Retained, SharedPool, SharedPoolLayout,
};
use thiserror::Error;

use crate::http::Body;

const GZIP_SLOTS: usize = 32;
const GZIP_CAPACITY: usize = 256 * 1024;

pub struct Gzip {
    inner: Option<GzipInner>,
    layout: SharedPoolLayout,
}

struct GzipInner {
    encoder: Compressor,
    pool: SharedPool<Initialized>,
}

impl Gzip {
    const LEVEL: i32 = 3;
    const COMPRESSION_LEVEL: CompressionLvl = match CompressionLvl::new(Self::LEVEL) {
        Ok(level) => level,
        Err(_) => CompressionLvl::fastest(),
    };

    pub fn new() -> Self {
        Self {
            inner: None,
            layout: SharedPoolLayout::fixed::<GZIP_SLOTS, GZIP_CAPACITY>(),
        }
    }

    pub fn with_pool(slots: usize, capacity: usize) -> Result<Self, PoolLayoutError> {
        Ok(Self {
            inner: None,
            layout: SharedPoolLayout::new(slots, capacity)?,
        })
    }

    fn inner(&mut self) -> &mut GzipInner {
        let layout = self.layout;
        self.inner.get_or_insert_with(|| GzipInner {
            encoder: Compressor::new(Self::COMPRESSION_LEVEL),
            pool: SharedPool::<Initialized>::from_layout(layout),
        })
    }

    pub fn encode(&mut self, src: &[u8]) -> Option<Pooled> {
        let inner = self.inner();
        let cap = inner.encoder.gzip_compress_bound(src.len());
        if cap > inner.pool.capacity() {
            return None;
        }
        let mut lease = inner.pool.try_acquire()?;
        let n = inner.encoder.gzip_compress(src, lease.spare_mut()).ok()?;
        lease.try_advance(n).ok()?;
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
    pool: SharedPool<Initialized>,
}

impl Gunzip {
    pub fn new() -> Self {
        Self {
            decoder: Decompressor::new(),
            pool: SharedPool::<Initialized>::from_layout(SharedPoolLayout::fixed::<
                GZIP_SLOTS,
                GZIP_CAPACITY,
            >()),
        }
    }

    pub fn with_pool(slots: usize, capacity: usize) -> Result<Self, PoolLayoutError> {
        Ok(Self {
            decoder: Decompressor::new(),
            pool: SharedPool::<Initialized>::from_layout(SharedPoolLayout::new(slots, capacity)?),
        })
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
                .map_err(|_| GunzipError::SizeLimit)?;
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
        let mut footer = [0; 4];
        footer.copy_from_slice(&src[src.len() - 4..]);
        Ok(u32::from_le_bytes(footer) as usize)
    }
}

impl Default for Gunzip {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::Gzip;

    #[test]
    fn gzip_allocates_its_pool_on_first_use() {
        let mut gzip = Gzip::new();
        assert!(gzip.inner.is_none());

        assert!(gzip.encode(b"hello").is_some());
        assert!(gzip.inner.is_some());
    }
}
