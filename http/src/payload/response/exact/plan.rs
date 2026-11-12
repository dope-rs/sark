use o3::{
    buffer::{
        storage,
        write::{Slice, consume},
    },
    mem::Rejected,
};

use crate::payload::response::{
    self,
    exact::{Body, Overflow, TryBody},
};

pub struct Plan<B> {
    body: B,
    len: usize,
}

impl<B> Plan<B>
where
    B: Body,
{
    pub fn new(body: B) -> Self {
        let len = body.encoded_len();
        Self { body, len }
    }
}

impl<B: TryBody> Plan<B> {
    /// Admits the complete length once while retaining the exact body owner.
    pub fn try_new(body: B) -> Result<Self, Rejected<B, Overflow>> {
        match body.try_len() {
            Ok(len) => Ok(Self { body, len }),
            Err(error) => Err(Rejected::new(body, error)),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn into_shared(self) -> Result<storage::Shared, storage::BuildError<consume::Error>> {
        let Self { body, len } = self;
        storage::Exact::try_build(len, |out| body.encode(out)).map(storage::Exact::into_shared)
    }

    pub fn encode<L: std::borrow::BorrowMut<usize>>(
        self,
        out: &mut Slice<'_, L>,
    ) -> Result<(), storage::BuildError<consume::Error>> {
        let Self {
            body,
            len: expected,
        } = self;
        let mut split = response::try_encode(out, expected, body)?;
        split
            .body()
            .try_complete()
            .map_err(storage::BuildError::Length)?;
        split.try_commit().map_err(storage::BuildError::Build)
    }
}
