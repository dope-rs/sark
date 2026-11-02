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
}

pub enum Output {
    Plain,
    Compressed(
        pool::Span<
            owners::Plain<pool::Fixed<{ Gzip::SLOTS.get() }, { Gzip::CAPACITY.get() }>>,
            pool::Owned,
            pool::Frozen,
        >,
    ),
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
}

impl Compression for Gzip {
    fn try_new() -> Result<Self, alloc::Error> {
        let encoder = encoder::Encoder::try_new()?;
        let pool = pool::Pool::try_from(pool::Fixed::new())?;
        Ok(Self { encoder, pool })
    }

    fn encode(&mut self, src: &[u8]) -> Output {
        if src.len() <= encoder::FRAMING {
            return Output::Plain;
        }
        let cap = self.encoder.bound(src.len());
        if cap > Self::CAPACITY.get() {
            return Output::Plain;
        }
        let Some(mut lease) = self.pool.acquire() else {
            return Output::Plain;
        };
        let Some(done) = self.encoder.encode(src, lease.output()) else {
            return Output::Plain;
        };
        done.commit();
        Output::Compressed(lease.freeze())
    }
}

impl Compression for () {
    fn try_new() -> Result<Self, alloc::Error> {
        Ok(())
    }

    fn encode(&mut self, _: &[u8]) -> Output {
        Output::Plain
    }
}
