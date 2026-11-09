use dope_core::random;
use o3::{buffer::hold, depot::capacity};

use crate::{
    life::session,
    manifold::{
        self,
        client::{self, io},
        schedule,
        schedule::{Error, Report, Terminal, Turn, data},
        stream,
    },
};

/// Scheduling change committed with one resident record mutation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Activation<B> {
    Keep,
    Data(Option<B>),
}

/// Whether terminal state remains resident after a drive turn.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    Keep,
    Remove,
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

/// Residency and failure committed by terminal settlement.
pub enum Settlement<E, K> {
    Keep,
    Remove,
    Failed {
        disposition: Disposition,
        error: Failure<E, K>,
    },
}

/// Payload-specific work executed inside a carrier domain.
pub trait Driver<C, T>
where
    C: client::Connection,
{
    type Error;

    fn data(
        &mut self,
        conn: &mut C,
        key: C::Key,
        value: &mut T,
    ) -> Result<Turn<C::Block>, Self::Error>;

    fn settle(
        &mut self,
        conn: &mut C,
        key: C::Key,
        value: &mut T,
        term: Terminal<Self::Error>,
    ) -> Settlement<Self::Error, C::Key>;

    fn removed(&mut self, value: T);
}

/// Drives resident client data while preserving the first settlement failure.
pub fn try_drive<C, T, D>(
    runtime: &mut schedule::Runtime<C::Key, T, C::Block>,
    connection: &mut C,
    driver: &mut D,
) -> Result<Report, Failure<D::Error, C::Key>>
where
    C: client::Connection,
    C::Block: schedule::Block,
    D: Driver<C, T>,
{
    let mut turn = Drive {
        connection,
        driver,
        failure: None,
    };
    let report = runtime.drive_data(&mut turn);
    match turn.failure {
        Some(error) => Err(error),
        None => Ok(report),
    }
}

struct Drive<'a, C: client::Connection, D, E> {
    connection: &'a mut C,
    driver: &'a mut D,
    failure: Option<Failure<E, C::Key>>,
}

impl<C, T, D> data::Driver<T> for Drive<'_, C, D, D::Error>
where
    C: client::Connection,
    C::Block: schedule::Block,
    D: Driver<C, T>,
{
    type Key = C::Key;
    type Block = C::Block;
    type Error = D::Error;

    fn can_continue(&self) -> bool {
        self.failure.is_none()
    }

    fn turn(&mut self, key: Self::Key, value: &mut T) -> Result<Turn<Self::Block>, Self::Error> {
        self.driver.data(self.connection, key, value)
    }

    fn settle(
        &mut self,
        key: Self::Key,
        value: &mut T,
        term: Terminal<Self::Error>,
    ) -> data::Settlement {
        match self.driver.settle(self.connection, key, value, term) {
            Settlement::Keep => data::Settlement::Keep,
            Settlement::Remove => data::Settlement::Remove,
            Settlement::Failed { disposition, error } => {
                self.failure = Some(error);
                match disposition {
                    Disposition::Keep => data::Settlement::Keep,
                    Disposition::Remove => data::Settlement::Remove,
                }
            }
        }
    }

    fn remove(&mut self, value: T) {
        self.driver.removed(value);
    }
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

    fn drive<D: Driver<C, T>>(
        &mut self,
        conn: &mut C,
        driver: &mut D,
    ) -> Result<Report, Failure<D::Error, C::Key>>;
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
