use std::{borrow::BorrowMut, convert::Infallible};

use o3::{
    buffer::{
        storage,
        write::{Slice, consume},
    },
    mem::Rejected,
};

use super::{Encode, bounded, exact};

/// The encoder and its admitted extent. An exact declaration must fill that extent.
pub struct Plan<B, const EXACT: bool> {
    body: B,
    limit: usize,
}

impl<B> Plan<B, true> {
    pub fn exact(body: B) -> Self
    where
        B: exact::Body,
    {
        let limit = body.encoded_len();
        Self { body, limit }
    }

    pub fn try_exact(body: B) -> Result<Self, Rejected<B, exact::Overflow>>
    where
        B: exact::TryBody,
    {
        match body.try_len() {
            Ok(limit) => Ok(Self { body, limit }),
            Err(error) => Err(Rejected::new(body, error)),
        }
    }
}

impl<B> Plan<B, false> {
    pub fn bounded(body: B) -> Self
    where
        B: bounded::Body,
    {
        let limit = body.max_len();
        Self { body, limit }
    }

    pub fn try_bounded(body: B) -> Result<Self, Rejected<B, B::Error>>
    where
        B: bounded::TryBody,
    {
        match body.try_max_len() {
            Ok(limit) => Ok(Self { body, limit }),
            Err(error) => Err(Rejected::new(body, error)),
        }
    }
}

impl<B: Encode, const EXACT: bool> Plan<B, EXACT> {
    pub const fn limit(&self) -> usize {
        self.limit
    }

    pub fn into_body(self) -> B {
        self.body
    }

    pub fn write<L: BorrowMut<usize>>(
        self,
        out: &mut Slice<'_, L>,
    ) -> Result<usize, storage::BuildError<consume::Error>> {
        let mut split = out
            .try_split(0, self.limit)
            .map_err(storage::BuildError::Build)?;
        self.body
            .encode(&mut split.body())
            .map_err(storage::BuildError::Build)?;
        let target = split.body();
        let written = if EXACT {
            target.try_complete().map_err(storage::BuildError::Length)?;
            self.limit
        } else {
            target.finish()
        };
        split.try_commit().map_err(storage::BuildError::Build)?;
        Ok(written)
    }

    pub fn into_shared(self) -> Result<storage::Shared, storage::BuildError<consume::Error>> {
        if EXACT {
            storage::Exact::try_build(self.limit, |out| self.body.encode(out))
                .map(storage::Exact::into_shared)
        } else {
            storage::Owned::try_build(self.limit, |out| self.body.encode(out))
                .map(storage::Owned::<()>::freeze)
        }
    }
}

/// Deferred declarations are measured only if the transport accepts a body.
pub trait Source<const EXACT: bool> {
    type Body: Encode;
    type Error: Into<exact::Overflow>;

    fn try_plan(self) -> Result<Plan<Self::Body, EXACT>, Self::Error>;
}

impl<B: exact::TryBody> Source<true> for B {
    type Body = B;
    type Error = exact::Overflow;

    fn try_plan(self) -> Result<Plan<B, true>, Self::Error> {
        Plan::try_exact(self).map_err(Rejected::discard)
    }
}

impl<B: bounded::Body> Source<false> for B {
    type Body = B;
    type Error = Infallible;

    fn try_plan(self) -> Result<Plan<B, false>, Self::Error> {
        Ok(Plan::bounded(self))
    }
}

impl<B: Encode, const EXACT: bool> Source<EXACT> for Plan<B, EXACT> {
    type Body = B;
    type Error = Infallible;

    fn try_plan(self) -> Result<Self, Self::Error> {
        Ok(self)
    }
}
