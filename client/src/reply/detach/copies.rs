use std::{mem, num::NonZero};

use o3::buffer::{
    bytes::window::Snapshot,
    resident, storage,
    write::{Sink as _, consume},
};

use crate::reply::{
    body,
    detach::{Error, Part, Parts},
};

/// Every error ends the consuming response operation; this builder is then dropped.
pub(crate) struct Copies {
    owner: Snapshot<{ u32::MAX as usize }>,
    total: u32,
    remaining: u32,
}

impl Copies {
    /// Successful window admission carries the exact u32-bounded copy total.
    pub(in crate::reply) fn try_new(bytes: usize, parts: Parts) -> Result<Self, Error> {
        let owner = match NonZero::new(bytes) {
            None => Snapshot::new(),
            Some(capacity) => match Snapshot::try_with(capacity) {
                Ok(owner) => owner,
                Err(storage::AllocError::Capacity(cause)) => {
                    return Err(Error::StorageCapacity { parts, cause });
                }
                Err(storage::AllocError::Exhausted(cause)) => {
                    return Err(Error::StorageExhausted {
                        parts,
                        requested: bytes,
                        cause,
                    });
                }
            },
        };
        Ok(Self {
            owner,
            total: bytes as u32,
            remaining: bytes as u32,
        })
    }

    pub(crate) fn try_retain<'d>(
        &mut self,
        owner: resident::Shared<'d>,
        part: Part,
    ) -> Result<storage::Shared, Error> {
        match owner.try_detach() {
            Ok(owner) => Ok(owner),
            Err(owner) => {
                let mut spare = self.owner.spare();
                let result = spare.write_slice(owner.as_slice());
                spare.commit();
                result.map_err(|cause| self.partition(part, cause))?;
                self.take(part)
            }
        }
    }

    pub(in crate::reply) fn flatten(
        &mut self,
        mut source: body::Scoped<'_, '_>,
    ) -> Result<storage::Shared, Error> {
        let mut spare = self.owner.spare();
        let result = source.segments.try_drain(&mut spare);
        spare.commit();
        result.map_err(|cause| self.partition(Part::Body, cause))?;
        self.take(Part::Body)
    }

    fn partition(&self, part: Part, cause: consume::Error) -> Error {
        let (need, _) = cause.into_parts();
        Error::Partition {
            part,
            offset: (self.total - self.remaining) as usize,
            requested: need.get(),
            capacity: self.total as usize,
        }
    }

    fn take(&mut self, part: Part) -> Result<storage::Shared, Error> {
        let len = self.owner.len() as u32;
        let owner = if len == self.remaining {
            mem::take(&mut self.owner).freeze()
        } else {
            let owner = self
                .owner
                .try_snapshot()
                .map_err(|cause| Error::Retain { part, cause })?
                .unwrap_or_default();
            self.owner.clear();
            owner
        };
        self.remaining -= len;
        Ok(owner)
    }
}

const _: () = assert!(std::mem::size_of::<Copies>() == 4 * std::mem::size_of::<usize>());
