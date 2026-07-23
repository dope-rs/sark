//! ```no_run
//! use sark_core::http::compress::Gzip;
//! let body: &[u8] = b"hello";
//! let zipped = Gzip::new().encode(body).unwrap();
//! ```

use libdeflater::{CompressionLvl, Compressor};
use o3::buffer::{InitializedSharedPool, Pooled};

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
