use o3::{
    buffer::{
        storage,
        write::{Sink, consume},
    },
    mem::Rejected,
};

use crate::payload::response::{
    self, Encode,
    bounded::{Body, TryBody},
};

pub struct Plan<B> {
    body: B,
    max_len: usize,
}

impl<B> Plan<B>
where
    B: Body,
{
    pub fn new(body: B) -> Self {
        let max_len = body.max_len();
        Self { body, max_len }
    }
}

impl<B: TryBody> Plan<B> {
    /// Admits the upper bound once while retaining the original encoder owner.
    pub fn try_new(body: B) -> Result<Self, Rejected<B, B::Error>> {
        match body.try_max_len() {
            Ok(max_len) => Ok(Self { body, max_len }),
            Err(error) => Err(Rejected::new(body, error)),
        }
    }

    pub fn max_len(&self) -> usize {
        self.max_len
    }

    pub fn into_shared(self) -> Result<storage::Shared, storage::BuildError<consume::Error>> {
        let Self { body, max_len } = self;
        storage::Owned::try_build(max_len, |out| body.encode(out))
            .map(storage::Owned::<(), 0>::freeze)
    }

    pub fn encode(self, out: &mut [u8]) -> Result<&mut [u8], storage::BuildError<consume::Error>> {
        let Self { body, max_len } = self;
        Ok(response::try_encode(out, max_len, body)?.into_mut())
    }
}

impl<B: TryBody> Body for Plan<B> {
    fn max_len(&self) -> usize {
        self.max_len
    }
}

impl<B: Encode> Encode for Plan<B> {
    fn encode<W>(self, out: &mut W) -> Result<(), consume::Error>
    where
        W: Sink<Error = consume::Error>,
    {
        self.body.encode(out)
    }
}
