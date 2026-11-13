use dope_core::random;
use o3::{buffer::hold, depot::capacity};

use crate::{
    life::session,
    manifold::{
        self,
        client::{self, io},
        schedule,
        schedule::{Error, Report, data},
        stream,
    },
};

/// Scheduling change committed with one resident record mutation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Activation<B> {
    Keep,
    Data(Option<B>),
}

/// Complete causal failure from one recoverable domain turn.
#[derive(Debug)]
pub enum Failure<E, K> {
    Turn(E),
    /// Resetting an idle record found this key inactive.
    Idle(K),
    /// The original failure and the key that was inactive during its reset.
    Settle {
        terminal: E,
        error: K,
    },
}

/// Carrier-selected resident storage and scheduling policy.
pub trait Domain<C, T>
where
    C: client::Connection,
{
    /// Fixed number of physical slots for this domain's lifetime.
    fn capacity(&self) -> usize;

    /// Returns the current complete identity at a stable physical slot.
    /// Removal and driving may vacate slots but must not move surviving keys
    /// between indices. Reinsertion may reuse a vacant slot with a new identity.
    fn key_at(&self, index: usize) -> Option<C::Key>;

    fn get(&self, stream: C::Key) -> Option<&T>;

    fn get_mut(&mut self, stream: C::Key) -> Option<&mut T>;

    fn remove(&mut self, stream: C::Key) -> Option<T>;

    fn has_work(&self) -> bool;

    fn try_insert(
        &mut self,
        stream: C::Key,
        make: impl FnOnce() -> T,
    ) -> schedule::InsertResult<C::Key, ()>;

    fn transaction<P, R, E>(
        &mut self,
        key: C::Key,
        check: impl FnOnce(&T) -> Result<(Activation<C::Block>, P), E>,
        commit: impl FnOnce(&mut T, P) -> R,
    ) -> Result<R, Error<E>>;

    fn resume(&mut self, conn: &mut C, egress: schedule::Egress);

    fn drive<D: data::Driver<C, T, Key = C::Key, Block = C::Block>>(
        &mut self,
        conn: &mut C,
        driver: &mut D,
    ) -> Result<Report, D::Failure>;
}

pub trait Plan<C: session::Storage>: Copy {
    type Key: manifold::raw::Key;
    type Block: Copy + Eq;
    type Permit;
    type Fields;
    type Selection;
    type Connection: client::Connection<
            Owner = C,
            Key = Self::Key,
            Block = Self::Block,
            Fields = Self::Fields,
            Selection = Self::Selection,
            Data: stream::Payload<C, Permit = Self::Permit>,
        > + io::Read
        + io::Write;
    type Domain<T>: Domain<Self::Connection, T>;
    type Build;
    type Storage;

    const RETENTION: hold::Policy;
    const MIN_PREFIX: usize;

    fn open(self) -> Result<Self::Connection, Self::Build>;

    /// Opens a connection from its driver's admitted entropy.
    fn open_seeded(self, seed: &random::Seed) -> Result<Self::Connection, Self::Build>;

    fn domain<T>(cap: capacity::Count) -> Result<Self::Domain<T>, Self::Storage>;
}
