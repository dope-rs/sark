//! Built-in gzip/brotli response compression. `with_thread_local` borrows a
//! per-thread encoder so a handler can compress a body without per-request
//! allocation; `encode` returns the compressed bytes. This is sark's standard
//! compression path — response bodies are never hand-rolled.
//!
//! ```no_run
//! use sark_core::http::compress::Gzip;
//! let body: &[u8] = b"hello";
//! let zipped = Gzip::with_thread_local(|gz| gz.encode(body).to_vec());
//! ```

use std::cell::RefCell;
use std::thread::LocalKey;

use libdeflater::{CompressionLvl, Compressor};

const BUF_CAP: usize = 64 * 1024;

fn with_slot<E, R>(slot: &'static LocalKey<RefCell<E>>, f: impl FnOnce(&mut E) -> R) -> R {
    slot.with(|cell| f(&mut cell.borrow_mut()))
}

pub struct Gzip {
    encoder: Compressor,
    buf: Vec<u8>,
}

impl Gzip {
    const LEVEL: i32 = 3;

    fn new(level: i32) -> Self {
        let lvl = CompressionLvl::new(level).unwrap_or(CompressionLvl::fastest());
        Self {
            encoder: Compressor::new(lvl),
            buf: Vec::with_capacity(BUF_CAP),
        }
    }

    pub fn encode(&mut self, src: &[u8]) -> &[u8] {
        let cap = self.encoder.gzip_compress_bound(src.len());
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
            static SLOT: RefCell<Gzip> = RefCell::new(Gzip::new(Gzip::LEVEL));
        }
        with_slot(&SLOT, f)
    }
}

/// Brotli response compression. `br` typically yields a smaller body than gzip
/// for text/JSON.
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
    /// `5`/`22`: compression-ratio vs throughput balance for runtime JSON.
    const QUALITY: i32 = 5;
    const LGWIN: i32 = 22;

    fn new(quality: i32, lgwin: i32) -> Self {
        let params = brotli::enc::BrotliEncoderParams {
            quality,
            lgwin,
            ..Default::default()
        };
        Self {
            params,
            buf: Vec::with_capacity(BUF_CAP),
        }
    }

    pub fn encode(&mut self, src: &[u8]) -> &[u8] {
        self.buf.clear();
        self.buf
            .reserve(brotli::enc::BrotliEncoderMaxCompressedSize(src.len()));
        let mut reader = src;
        brotli::BrotliCompress(&mut reader, &mut self.buf, &self.params)
            .expect("brotli compress into Vec is infallible");
        &self.buf
    }

    pub fn with_thread_local<R>(f: impl FnOnce(&mut Brotli) -> R) -> R {
        thread_local! {
            static SLOT: RefCell<Brotli> = RefCell::new(Brotli::new(Brotli::QUALITY, Brotli::LGWIN));
        }
        with_slot(&SLOT, f)
    }
}
