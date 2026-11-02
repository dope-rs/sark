use std::marker;

use dope::core::random;
use o3::{
    alloc,
    buffer::{hold, pool, pool::owners},
    depot::capacity,
};
use sark_core::{
    life::session,
    manifold::{client::domain, schedule},
};
use sark_http::{field::metadata::section::rules, payload::compress::dynamic::hash};

use crate::{
    ClientRole, INGRESS_RETENTION,
    client::Domain,
    conn::{self, config, event, machine},
    stream::Key,
};

/// Validated storage and connection plan for one H2 client.
pub struct Plan<F = rules::RetainRaw> {
    config: config::Validated<ClientRole>,
    marker: marker::PhantomData<fn() -> F>,
}

impl<F> Copy for Plan<F> {}

impl<F> Clone for Plan<F> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<F> Plan<F> {
    pub fn new(slots: usize) -> Result<Self, config::Error> {
        let raw = config::Config {
            stream_capacity: slots,
            ..config::Config::default()
        };
        Ok(Self::from(config::Validated::new(raw)?))
    }

    /// Exact byte capacity admitted for this connection's input queue.
    #[doc(hidden)]
    pub const fn inbound_capacity(&self) -> usize {
        self.config.inbound_capacity()
    }
}

impl<F> From<config::Validated<ClientRole>> for Plan<F> {
    /// Retains an already validated connection configuration without checking it again.
    fn from(config: config::Validated<ClientRole>) -> Self {
        Self {
            config,
            marker: marker::PhantomData,
        }
    }
}

impl<F: rules::Policy, C: session::Storage> domain::Plan<C> for Plan<F> {
    type Key = Key;
    type Block = conn::DataBlock;
    type Permit = pool::Span<owners::Plain<pool::Width<1>>, pool::Owned, pool::Frozen>;
    type Fields = F::Block;
    type Selection = F::Selection;
    type Connection = machine::Connection<ClientRole, F, C>;
    type Domain<T> = Domain<T, F>;
    type Build = conn::Build;
    type Storage = alloc::Error;

    const RETENTION: hold::Policy = INGRESS_RETENTION;
    const MIN_PREFIX: usize = event::MIN_RETAINED_WRITE_PREFIX_CAPACITY;

    fn open(self) -> Result<Self::Connection, Self::Build> {
        machine::Connection::from_plan(self.config)
    }

    fn open_seeded(self, seed: &random::Seed) -> Result<Self::Connection, Self::Build> {
        machine::Connection::from_seed(self.config, hash::Seed::new(seed))
    }

    fn domain<T>(cap: capacity::Count) -> Result<Self::Domain<T>, Self::Storage> {
        Ok(Domain {
            inner: schedule::Runtime::try_bounded(cap, cap)?,
            marker: marker::PhantomData,
        })
    }
}

const _: () = assert!(!core::mem::needs_drop::<Plan>());
const _: () =
    assert!(core::mem::size_of::<Plan>() == core::mem::size_of::<config::Validated<ClientRole>>());
