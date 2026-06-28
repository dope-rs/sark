//! Built-in gzip response compression (libdeflater). `Gzip::with_thread_local`
//! borrows a per-thread encoder (level 3) so a handler can compress a body when
//! `Accept-Encoding` includes `gzip` without per-request allocation; `encode`
//! returns the gzip bytes. This is sark's standard compression path — response
//! bodies are never hand-rolled.
//!
//! ```no_run
//! use sark_core::http::compress::Gzip;
//! let body: &[u8] = b"hello";
//! let zipped = Gzip::with_thread_local(|gz| gz.encode(body).to_vec());
//! ```

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

/// Brotli response compression. `br` typically yields a smaller body than gzip
/// for text/JSON. `with_thread_local` borrows a per-thread encoder so a handler
/// compresses without per-request setup; `encode` returns the brotli bytes.
///
/// ```no_run
/// use sark_core::http::compress::Brotli;
/// let body: &[u8] = b"hello";
/// let zipped = Brotli::with_thread_local(|br| br.encode(body).to_vec());
/// ```
pub struct Brotli {
    params: brotli::enc::BrotliEncoderParams,
    buf: Vec<u8>,
}

impl Brotli {
    fn new(quality: i32, lgwin: i32) -> Self {
        let params = brotli::enc::BrotliEncoderParams {
            quality,
            lgwin,
            ..Default::default()
        };
        Self {
            params,
            buf: Vec::with_capacity(64 * 1024),
        }
    }

    pub fn encode(&mut self, src: &[u8]) -> &[u8] {
        self.buf.clear();
        let mut reader = src;
        brotli::BrotliCompress(&mut reader, &mut self.buf, &self.params)
            .expect("brotli compress into Vec is infallible");
        &self.buf
    }

    pub fn with_thread_local<R>(f: impl FnOnce(&mut Brotli) -> R) -> R {
        // quality 5 / window 22: ratio/throughput balance for runtime JSON.
        thread_local! {
            static SLOT: RefCell<Brotli> = RefCell::new(Brotli::new(5, 22));
        }
        SLOT.with(|cell| f(&mut cell.borrow_mut()))
    }
}
