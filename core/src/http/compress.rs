use std::cell::RefCell;

use libdeflater::{CompressionLvl, Compressor};

pub struct Gzip {
    encoder: Compressor,
    buf: Vec<u8>,
}

impl Gzip {
    fn new(level: i32) -> Self {
        let lvl = CompressionLvl::new(level).unwrap_or(CompressionLvl::fastest());
        Self {
            encoder: Compressor::new(lvl),
            buf: Vec::with_capacity(64 * 1024),
        }
    }

    pub fn encode(&mut self, src: &[u8]) -> &[u8] {
        let cap = self.encoder.gzip_compress_bound(src.len());
        if self.buf.capacity() < cap {
            self.buf.reserve(cap - self.buf.capacity());
        }
        self.buf.resize(cap, 0);
        let n = self
            .encoder
            .gzip_compress(src, &mut self.buf)
            .expect("gzip_compress_bound undersized");
        self.buf.truncate(n);
        &self.buf
    }

    pub fn with_thread_local<R>(f: impl FnOnce(&mut Gzip) -> R) -> R {
        thread_local! {
            static SLOT: RefCell<Gzip> = RefCell::new(Gzip::new(3));
        }
        SLOT.with(|cell| f(&mut cell.borrow_mut()))
    }
}
