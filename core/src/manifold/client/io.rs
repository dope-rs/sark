use dope_net::link::egress::{
    NonEmpty,
    data::pair,
    load::{self, lease},
};
use o3::buffer::{bytes::bounded::Overflow, pool, write::consume};

use crate::{life::session, manifold::client};

/// Result of one atomic DATA write attempt.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Outcome<B> {
    Written(usize),
    Blocked(B),
}

/// Bytes removed from protocol staging by one committed write.
#[must_use = "the returned write owns bytes removed from its session"]
pub enum Output<C: session::Storage> {
    Buffered(usize),
    Split { prefix_len: usize, body: C },
}

/// Rejected final-link preparation retaining the reusable prefix owner.
pub enum Reject {
    Empty(pool::Cursor),
    Failed { cause: Error, prefix: pool::Cursor },
}

/// Exact preparation failure after its reusable prefix has been returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Capacity(consume::Error),
    Overflow(Overflow),
    Body(load::Error),
}

const _: () = {
    assert!(size_of::<Reject>() == size_of::<pool::Cursor>() + size_of::<Error>());
    assert!(size_of::<Error>() == 3 * size_of::<usize>());
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Prefix(load::Error),
    Body(pair::Error),
}

pub enum Offer<P: lease::Commit, B: lease::Commit> {
    Prefix(NonEmpty<lease::Lease<P>>),
    Body(NonEmpty<lease::Lease<B>>),
}

/// Owns the actual wire payload while borrowing its unchanged protocol state.
pub trait Prepared<C: session::Storage>: Sized {
    type Prefix: lease::Commit<Item = pool::Cursor>;
    type Body: lease::Commit<Item = pair::Source<C>>;

    fn try_bind(self) -> Result<Offer<Self::Prefix, Self::Body>, Failure>;
}

/// Incremental ingress owned by one client connection.
pub trait Read: client::Connection {
    fn has_event(&mut self) -> bool;

    fn is_blocked(&mut self) -> bool;

    fn ingest(&mut self, bytes: &[u8]) -> Result<(), Self::Error>;

    fn ingest_owned(&mut self, bytes: Self::Owner) -> Result<(), Self::Error>;

    fn ingest_from<I>(&mut self, source: &mut I) -> Result<(), Self::Error>
    where
        I: Iterator<Item = Self::Owner>;

    fn resume(&mut self) -> Result<(), Self::Error>;
}

/// Final-link output owned by one client connection.
pub trait Write: client::Connection {
    type Prepared<'a>: Prepared<Self::Owner>
    where
        Self: 'a;

    fn outbound(&self) -> &[u8];

    fn outbound_len(&self) -> usize;

    fn drain(&mut self, len: usize);

    fn take(&mut self, prefix: &mut [u8]) -> Result<Output<Self::Owner>, Error>;

    fn prepare(&mut self, prefix: pool::Cursor) -> Result<Self::Prepared<'_>, Reject>;
}
