//! ```no_run
//! use sark_core::http::compress::Gzip;
//! let body: &[u8] = b"hello";
//! let zipped = Gzip::new().encode(body).unwrap();
//! ```

use std::ptr::NonNull;

use libdeflate_sys::{
    libdeflate_alloc_compressor, libdeflate_compressor, libdeflate_free_compressor,
    libdeflate_gzip_compress, libdeflate_gzip_compress_bound,
};
use o3::buffer::{Pooled, SharedPool};

const GZIP_SLOTS: usize = 32;
const GZIP_CAPACITY: usize = 256 * 1024;

pub struct Gzip {
    encoder: NonNull<libdeflate_compressor>,
    pool: SharedPool,
}

impl Gzip {
    const LEVEL: i32 = 3;

    pub fn new() -> Self {
        Self::with_pool(GZIP_SLOTS, GZIP_CAPACITY)
    }

    pub fn with_pool(slots: usize, capacity: usize) -> Self {
        let encoder = NonNull::new(unsafe { libdeflate_alloc_compressor(Self::LEVEL) })
            .expect("libdeflate compressor allocation failed");
        Self {
            encoder,
            pool: SharedPool::new(slots, capacity),
        }
    }

    pub fn encode(&mut self, src: &[u8]) -> Option<Pooled> {
        let cap = unsafe { libdeflate_gzip_compress_bound(self.encoder.as_ptr(), src.len()) };
        if cap > self.pool.capacity() {
            return None;
        }
        let mut lease = self.pool.try_acquire()?;
        let mut writer = lease.spare_writer();
        let ptr = writer.as_mut_ptr();
        let capacity = writer.remaining();
        let n = unsafe {
            libdeflate_gzip_compress(
                self.encoder.as_ptr(),
                src.as_ptr().cast(),
                src.len(),
                ptr.cast(),
                capacity,
            )
        };
        assert!(n != 0 && n <= capacity, "gzip_compress_bound undersized");
        let output = unsafe { std::slice::from_raw_parts(ptr, n) };
        writer
            .try_commit_initialized(output)
            .expect("gzip_compress_bound undersized");
        drop(writer);
        Some(lease.freeze())
    }
}

impl Drop for Gzip {
    fn drop(&mut self) {
        unsafe { libdeflate_free_compressor(self.encoder.as_ptr()) };
    }
}

impl Default for Gzip {
    fn default() -> Self {
        Self::new()
    }
}
