use miniz_oxide::deflate::core::TDEFLStatus;
use o3::buffer::pool::owners;

#[doc(hidden)]
pub mod dynamic;
mod encoder;
pub mod inflate;

use std::num;

use o3::{
    alloc,
    buffer::pool::{self, state},
};

/// Compression storage selected when constructing a protocol connection.
pub trait Compression: Sized {
    fn try_new() -> Result<Self, alloc::Error>;

    /// Returns a smaller gzip body or keeps the original bytes unchanged.
    fn encode(&mut self, bytes: &[u8]) -> Output;

    fn try_encode(&mut self, bytes: &[u8]) -> Result<Frozen, Error>;
}

#[doc(hidden)]
pub type Frozen = pool::Span<
    owners::Plain<pool::Fixed<{ Gzip::SLOTS.get() }, { Gzip::CAPACITY.get() }>>,
    pool::Owned,
    pool::Frozen,
>;

pub enum Output {
    Plain,
    Compressed(Frozen),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Unavailable,
    Input {
        actual: usize,
        limit: usize,
    },
    Exhausted,
    Deflate {
        status: TDEFLStatus,
        read: usize,
        wrote: usize,
    },
    Extent {
        wrote: usize,
        capacity: usize,
        maximum: usize,
    },
}

pub struct Gzip {
    encoder: encoder::Encoder,
    pool: pool::Pool<
        state::Initialized,
        owners::Plain<pool::Fixed<{ Gzip::SLOTS.get() }, { Gzip::CAPACITY.get() }>>,
    >,
}

impl Gzip {
    pub const SLOTS: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(31);
    pub const CAPACITY: num::NonZero<usize> =
        num::NonZero::<usize>::MIN.saturating_add(256 * 1024 - 1);

    fn try_compress(&mut self, src: &[u8], maximum: usize) -> Result<Frozen, Error> {
        let mut lease = self.pool.acquire().ok_or(Error::Exhausted)?;
        self.encoder.encode(src, lease.output(), maximum)?;
        Ok(lease.freeze())
    }
}

impl Compression for Gzip {
    fn try_new() -> Result<Self, alloc::Error> {
        let encoder = encoder::Encoder::try_new()?;
        let pool = pool::Pool::try_from(pool::Fixed::new())?;
        Ok(Self { encoder, pool })
    }

    fn encode(&mut self, src: &[u8]) -> Output {
        if !(encoder::FRAMING + 1..=encoder::MAX_INPUT).contains(&src.len()) {
            return Output::Plain;
        }
        match self.try_compress(src, src.len() - 1) {
            Ok(frozen) => Output::Compressed(frozen),
            Err(_) => Output::Plain,
        }
    }

    fn try_encode(&mut self, src: &[u8]) -> Result<Frozen, Error> {
        if src.len() > encoder::MAX_INPUT {
            return Err(Error::Input {
                actual: src.len(),
                limit: encoder::MAX_INPUT,
            });
        }
        self.try_compress(src, Self::CAPACITY.get())
    }
}

impl Compression for () {
    fn try_new() -> Result<Self, alloc::Error> {
        Ok(())
    }

    fn encode(&mut self, _: &[u8]) -> Output {
        Output::Plain
    }

    fn try_encode(&mut self, _: &[u8]) -> Result<Frozen, Error> {
        Err(Error::Unavailable)
    }
}
