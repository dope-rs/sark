use std::convert::Infallible;

use o3::base::cell::region;
use o3::buffer::segments;

pub mod raw;
mod seal;
pub(in crate::body::credit) use seal::Seal;

/// An unforgeable zero-sized account identity or its private persisted form.
pub trait Origin: Seal + Copy {}

impl Origin for raw::Stored {}
impl<'id> Origin for region::Proof<region::Identity<'id>> {}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Amount {
    bytes: usize,
    chunks: segments::Count,
}

impl Amount {
    pub const fn new(bytes: usize, chunks: segments::Count) -> Self {
        Self { bytes, chunks }
    }

    pub const fn bytes(self) -> usize {
        self.bytes
    }

    pub const fn chunks(self) -> segments::Count {
        self.chunks
    }

    pub const fn is_empty(self) -> bool {
        self.bytes == 0 && self.chunks.is_empty()
    }

    fn try_add(self, added: Self, scope: Scope) -> Result<Self, Error> {
        let bytes = self
            .bytes
            .checked_add(added.bytes)
            .ok_or(Error::ByteOverflow {
                scope,
                current: self.bytes,
                added: added.bytes,
            })?;
        let chunks = self
            .chunks
            .checked_add(added.chunks)
            .ok_or(Error::ChunkOverflow {
                scope,
                current: self.chunks,
                added: added.chunks,
            })?;
        Ok(Self { bytes, chunks })
    }

    fn try_fit(self, max: Limits, scope: Scope) -> Result<Self, Error> {
        if self.bytes > max.bytes {
            return Err(Error::ByteLimit {
                scope,
                actual: self.bytes,
                limit: max.bytes,
            });
        }
        if u128::from(self.chunks.into_raw()) > max.chunks as u128 {
            return Err(Error::ChunkLimit {
                scope,
                actual: self.chunks,
                limit: max.chunks,
            });
        }
        Ok(self)
    }

    fn try_sub(self, removed: Self, scope: Scope) -> Result<Self, Error> {
        let bytes = self
            .bytes
            .checked_sub(removed.bytes)
            .ok_or(Error::ByteUnderflow {
                scope,
                current: self.bytes,
                removed: removed.bytes,
            })?;
        let chunks = self
            .chunks
            .checked_sub(removed.chunks)
            .ok_or(Error::ChunkUnderflow {
                scope,
                current: self.chunks,
                removed: removed.chunks,
            })?;
        Ok(Self { bytes, chunks })
    }
}

impl Default for Amount {
    fn default() -> Self {
        Self::new(0, segments::Count::EMPTY)
    }
}

/// Ownership disposition leaving pre-admission accounting.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    Discarded,
    Admitted,
}

/// A committed body mutation exposed to lifecycle evidence.
#[derive(PartialEq, Eq)]
pub enum Mutation {
    Head,
    Reserved(Amount),
    Released {
        credit: Amount,
        disposition: Disposition,
    },
    BuildReady,
}

impl Mode for Mutation {
    fn committed(change: Mutation) -> Option<Self> {
        Some(change)
    }

    fn credit(bytes: usize, chunks: segments::Count) -> Amount {
        Amount::new(bytes, chunks)
    }
}

/// Constructs the committed mutation of one event for the receiver's event type.
///
/// A statically disabled topology reports `None` from `committed`, so its
/// uninhabited `Change` value is never built and no recording call can occur.
pub trait Mode: Sized {
    /// Wraps a mutation this receiver has already built, or `None` when this
    /// topology records nothing.
    fn committed(change: Mutation) -> Option<Self>;

    fn head() -> Option<Self> {
        Self::committed(Mutation::Head)
    }

    fn reserved(credit: Amount) -> Option<Self> {
        (!credit.is_empty())
            .then_some(Mutation::Reserved(credit))
            .and_then(Self::committed)
    }

    fn released(credit: Amount, cause: Disposition) -> Option<Self> {
        (!credit.is_empty())
            .then_some(Mutation::Released {
                credit,
                disposition: cause,
            })
            .and_then(Self::committed)
    }

    fn build_ready() -> Option<Self> {
        Self::committed(Mutation::BuildReady)
    }

    fn credit(bytes: usize, chunks: segments::Count) -> Amount;
}

impl Mode for Infallible {
    fn committed(_: Mutation) -> Option<Self> {
        None
    }

    fn credit(_: usize, _: segments::Count) -> Amount {
        Amount::default()
    }
}

/// Receives committed body mutations with statically selected evidence.
pub trait Evidence {
    type Change: Mode;

    fn record(&mut self, change: Self::Change);

    fn head(&mut self) {
        if let Some(change) = Self::Change::head() {
            self.record(change);
        }
    }

    fn credit(bytes: usize, chunks: segments::Count) -> Amount
    where
        Self: Sized,
    {
        Self::Change::credit(bytes, chunks)
    }

    fn reserved(&mut self, credit: Amount) {
        if let Some(change) = Self::Change::reserved(credit) {
            self.record(change);
        }
    }

    fn released(&mut self, credit: Amount, kind: Disposition) {
        if let Some(change) = Self::Change::released(credit, kind) {
            self.record(change);
        }
    }

    fn build_ready(&mut self) {
        if let Some(change) = Self::Change::build_ready() {
            self.record(change);
        }
    }
}

impl<E: Evidence + ?Sized> Evidence for &mut E {
    type Change = E::Change;

    fn record(&mut self, change: Self::Change) {
        E::record(self, change);
    }
}

/// Zero-sized evidence for disabled production topologies.
///
#[derive(Clone, Copy, Default)]
pub struct Disabled;

impl Disabled {
    pub const fn new() -> Self {
        Self
    }
}

impl Evidence for Disabled {
    type Change = Infallible;

    /// `Infallible` admits no value: eliminating it is the whole proof, and it
    /// compiles to no instructions.
    fn record(&mut self, change: Infallible) {
        match change {}
    }
}

/// Remaining pre-admission credit detached from its ingress state.
#[must_use = "released credit must be recorded with its final disposition"]
#[derive(Debug)]
pub struct Release<O: Origin = raw::Stored> {
    pub(in crate::body) amount: Amount,
    _origin: O,
}

/// Committed connection-credit disposition after a body is consumed.
#[must_use = "a rejected release must be explicitly abandoned"]
pub enum Discard<O: Origin = raw::Stored> {
    Applied,
    Rejected { release: Release<O>, error: Error },
}

impl<O: Origin> Discard<O> {
    /// Abandons rejected credit and returns its original settlement error.
    pub fn abandon<E: Evidence>(self, evidence: E) -> Result<(), Error> {
        match self {
            Self::Applied => Ok(()),
            Self::Rejected { release, error } => {
                release.discard(evidence);
                Err(error)
            }
        }
    }
}

impl<O: Origin> Release<O> {
    pub(in crate::body) const fn new(amount: Amount, origin: O) -> Self {
        Self {
            amount,
            _origin: origin,
        }
    }

    pub fn try_discard<E: Evidence>(
        mut self,
        ledger: &mut raw::Ledger<'_, O>,
        mut evidence: E,
    ) -> Discard<O> {
        let change = match ledger.amount().try_drain(&mut self.amount) {
            Ok(change) => change,
            Err(error) => {
                return Discard::Rejected {
                    release: self,
                    error,
                };
            }
        };
        let freed = change.commit();
        evidence.released(freed, Disposition::Discarded);
        Discard::Applied
    }

    /// Records abandonment when the connection ledger can no longer be settled.
    pub fn discard<E: Evidence>(self, mut evidence: E) {
        evidence.released(self.amount, Disposition::Discarded);
    }
}

/// Byte and owner-count ceilings for one credit domain.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub bytes: usize,
    pub chunks: usize,
}

impl Limits {
    pub const fn new(bytes: usize, chunks: usize) -> Self {
        Self { bytes, chunks }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Scope {
    Request,
    Connection,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    ByteOverflow {
        scope: Scope,
        current: usize,
        added: usize,
    },
    ChunkOverflow {
        scope: Scope,
        current: segments::Count,
        added: segments::Count,
    },
    ByteLimit {
        scope: Scope,
        actual: usize,
        limit: usize,
    },
    ChunkLimit {
        scope: Scope,
        actual: segments::Count,
        limit: usize,
    },
    ByteUnderflow {
        scope: Scope,
        current: usize,
        removed: usize,
    },
    ChunkUnderflow {
        scope: Scope,
        current: segments::Count,
        removed: segments::Count,
    },
}

/// A fully validated body-credit mutation with exclusive commit authority.
pub(in crate::body) struct Change<'a> {
    request: &'a mut Amount,
    connection: &'a mut Amount,
    next_request: Amount,
    next_connection: Amount,
    changed: Amount,
}

impl Change<'_> {
    pub(in crate::body) fn commit(self) -> Amount {
        *self.request = self.next_request;
        *self.connection = self.next_connection;
        self.changed
    }
}

impl Amount {
    pub(in crate::body) fn try_commit<'a>(
        &'a mut self,
        req: &'a mut Amount,
        amount: Amount,
        rmax: Limits,
        cmax: Limits,
    ) -> Result<Change<'a>, Error> {
        let next = req.try_add(amount, Scope::Request)?;
        let total = self.try_add(amount, Scope::Connection)?;
        let next = next.try_fit(rmax, Scope::Request)?;
        let total = total.try_fit(cmax, Scope::Connection)?;

        Ok(Change {
            request: req,
            connection: self,
            next_request: next,
            next_connection: total,
            changed: amount,
        })
    }

    pub(in crate::body) fn try_release<'a>(
        &'a mut self,
        req: &'a mut Amount,
        amount: Amount,
    ) -> Result<Change<'a>, Error> {
        let next = req.try_sub(amount, Scope::Request)?;
        let total = self.try_sub(amount, Scope::Connection)?;
        Ok(Change {
            request: req,
            connection: self,
            next_request: next,
            next_connection: total,
            changed: amount,
        })
    }

    pub(in crate::body) fn try_drain<'a>(
        &'a mut self,
        req: &'a mut Amount,
    ) -> Result<Change<'a>, Error> {
        let freed = *req;
        self.try_release(req, freed)
    }
}

const _: () = {
    assert!(core::mem::size_of::<Amount>() == 2 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Release>() == core::mem::size_of::<Amount>());
    assert!(
        core::mem::size_of::<Release<region::Proof<region::Identity<'static>>>>()
            == core::mem::size_of::<Amount>()
    );
    assert!(core::mem::size_of::<Disabled>() == 0);
};
