use std::{iter, num::NonZero};

use dope_core::random;
use sark_http::payload::compress::dynamic::{
    ENTRY_OVERHEAD, hash,
    insert::{self, write},
    mode, table,
};

pub const NAME: &[u8] = b"x";
pub const VALID: &[u8] = b"a";
pub const INVALID: &[u8] = b"a\0";
pub const SPACE: &[u8] = b" ";

#[derive(Debug)]
pub enum Error {
    Begin(insert::Capacity),
    Write(write::Error),
    Finish(insert::Error),
}

pub trait Acquire: Sized {
    fn try_acquire() -> Result<Self, dope_core::Errno>;
}

impl Acquire for hash::Seed {
    fn try_acquire() -> Result<Self, dope_core::Errno> {
        let seed = random::Seed::acquire()?;
        Ok(Self::new(&seed))
    }
}

pub trait Insert {
    fn try_insert(&mut self, name: &[u8], value: &[u8]) -> Result<(), Error>;
}

impl Insert for table::Table<mode::Staged> {
    fn try_insert(&mut self, name: &[u8], value: &[u8]) -> Result<(), Error> {
        let mut writer = self.try_begin().map_err(Error::Begin)?;
        writer.push(name).map_err(Error::Write)?;
        let mut writer = writer
            .finish()
            .map_err(|rejected| Error::Finish(rejected.discard()))?;
        writer.push(value).map_err(Error::Write)?;
        writer.commit();
        Ok(())
    }
}

pub trait Model {
    fn trim(&mut self, limit: usize, incoming: usize);
    fn size(&self) -> usize;
}

impl Model for Vec<(Vec<u8>, Vec<u8>)> {
    fn trim(&mut self, limit: usize, incoming: usize) {
        while self.size().saturating_add(incoming) > limit {
            if self.pop().is_none() {
                break;
            }
        }
    }

    fn size(&self) -> usize {
        self.iter()
            .map(|(name, value)| ENTRY_OVERHEAD + name.len() + value.len())
            .sum()
    }
}

pub struct Random(u64);

impl Random {
    pub const fn new(seed: u64) -> Self {
        Self(seed)
    }
    pub fn bytes(&mut self, bound: NonZero<usize>) -> Vec<u8> {
        let len = self.advance() % bound.get();
        iter::repeat_with(|| b'a' + (self.advance() % 26) as u8)
            .take(len)
            .collect()
    }

    pub fn advance(&mut self) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 as usize
    }
}
