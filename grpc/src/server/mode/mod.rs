//! Statically selected gRPC server modes.
use std::{marker, mem, num::NonZero};

use dope::core::driver::settings::{self, claim, errors};
use o3::{
    buffer::{resident, resident::bound::Unbounded, storage},
    depot::capacity::Count,
};
use sark_core::manifold::server::{Connection, app};
use sark_h2::stream::Key;

use crate::{
    protocol::section,
    server::{Bank, runtime::run},
};

mod direct;
mod error;
mod limit;
pub mod schedule;

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
pub struct Demand<R, S> {
    capacity: NonZero<u32>,
    mode: marker::PhantomData<fn() -> (R, S)>,
}

impl<R, S> Demand<R, S> {
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

impl<R, S> settings::Resource for Demand<R, S> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

const _: () = {
    assert!(mem::size_of::<Demand<(), StreamTag>>() == mem::size_of::<NonZero<u32>>());
    assert!(mem::align_of::<Demand<(), StreamTag>>() == mem::align_of::<NonZero<u32>>());
    assert!(!mem::needs_drop::<Demand<(), StreamTag>>());
    assert!(mem::size_of::<Demand<(), UnaryTag>>() == mem::size_of::<NonZero<u32>>());
    assert!(mem::align_of::<Demand<(), UnaryTag>>() == mem::align_of::<NonZero<u32>>());
    assert!(!mem::needs_drop::<Demand<(), UnaryTag>>());
};

#[doc(hidden)]
pub trait Build<'a, 'd, H, C, O>: settings::Resource
where
    C: Connection<
            Key = Key,
            Owner = resident::Shared<'d>,
            Fields = section::Owner,
            Output = storage::Shared,
        >,
    O: Copy + Eq + 'd,
{
    type Application: app::Application<
            'd,
            C,
            O,
            Fields = section::Plan,
            Failure = run::Failure,
            Egress<'d> = Unbounded,
        >;
    type Error;

    fn build(self, app: H, bank: &'a Bank) -> Result<Self::Application, Self::Error>;
}
