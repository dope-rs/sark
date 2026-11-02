//! Statically selected gRPC server modes.
use std::{marker, mem, num::NonZero};

use dope::core::driver::settings::{self, claim, errors};
use o3::depot::capacity::Count;

mod direct;
mod error;
mod limit;

pub use direct::Direct;
pub use error::Error;
pub use limit::Limit;

/// Selects the streaming server mode.
pub enum StreamTag {}

/// Selects the unary server mode.
pub enum UnaryTag {}

/// One server mode's admitted demand on the connection bank.
///
/// `S` selects the mode, so the unary and streaming demands stay distinct
/// types while sharing this record and its policy.
#[derive(Clone, Copy)]
pub struct Demand<S> {
    capacity: NonZero<u32>,
    mode: marker::PhantomData<fn() -> S>,
}

impl<S> Demand<S> {
    pub fn try_new(cap: usize) -> Result<Self, Error> {
        let Some(capacity) = NonZero::<usize>::new(cap) else {
            return Err(Error::Capacity {
                requested: cap,
                cause: Limit::Zero,
            });
        };
        let capacity = Count::try_nonzero(capacity).map_err(|_| Error::Capacity {
            requested: cap,
            cause: Limit::Domain,
        })?;
        Ok(Self {
            capacity,
            mode: marker::PhantomData,
        })
    }

    pub(in crate::server) const fn capacity(&self) -> NonZero<u32> {
        self.capacity
    }
}

impl<S> settings::Resource for Demand<S> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

const _: () = {
    assert!(mem::size_of::<Demand<StreamTag>>() == mem::size_of::<NonZero<u32>>());
    assert!(mem::align_of::<Demand<StreamTag>>() == mem::align_of::<NonZero<u32>>());
    assert!(!mem::needs_drop::<Demand<StreamTag>>());
    assert!(mem::size_of::<Demand<UnaryTag>>() == mem::size_of::<NonZero<u32>>());
    assert!(mem::align_of::<Demand<UnaryTag>>() == mem::align_of::<NonZero<u32>>());
    assert!(!mem::needs_drop::<Demand<UnaryTag>>());
};
