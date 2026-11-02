//! Packed metadata maps and their borrowed records.

use std::mem;

use metadata::packed::{Iter, RangeIter, prefix::Prefix};
use o3::{
    alloc,
    alloc::{BoxSliceExt as _, VecExt as _},
    buffer::write::{self, Sink as _, consume},
};
use record::Entry;

use crate::field::metadata;

pub mod record;

#[derive(PartialEq, Eq)]
pub struct Owned {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
    pub indexing: crate::field::Indexing,
}

impl Owned {
    pub fn try_new(name: &[u8], value: &[u8]) -> Result<Self, alloc::Exhausted> {
        Ok(Self {
            name: Box::<[u8]>::try_copy(name)?.into_vec(),
            value: Box::<[u8]>::try_copy(value)?.into_vec(),
            indexing: crate::field::Indexing::Allowed,
        })
    }

    pub fn as_ref(&self) -> Entry<'_> {
        Entry::new(&self.name, &self.value).with_index(self.indexing)
    }

    pub fn with_index(mut self, indexing: crate::field::Indexing) -> Self {
        self.indexing = indexing;
        self
    }
}

impl TryFrom<Entry<'_>> for Owned {
    type Error = alloc::Exhausted;

    fn try_from(field: Entry<'_>) -> Result<Self, Self::Error> {
        Ok(Self::try_new(field.name, field.value)?.with_index(field.indexing))
    }
}

#[derive(Clone)]
pub struct OwnedIter<'a> {
    fields: &'a [Owned],
}

impl<'a> Iterator for OwnedIter<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let (field, rest) = self.fields.split_first()?;
        self.fields = rest;
        Some(field.as_ref())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.fields.len();
        (len, Some(len))
    }
}

impl ExactSizeIterator for OwnedIter<'_> {}

impl metadata::Metadata for Vec<Owned> {
    type Iter<'a> = OwnedIter<'a>;

    fn fields(&self) -> Self::Iter<'_> {
        OwnedIter {
            fields: self.as_slice(),
        }
    }
}

/// One contiguous packed field block with no secondary-owner slot.
#[derive(PartialEq, Eq)]
pub struct Fields<S> {
    bytes: S,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Reserve(alloc::Error),
    Write(consume::Error),
    ComponentTooLarge {
        len: usize,
    },
    CapacityOverflow {
        current: usize,
        name: usize,
        value: usize,
    },
    ValueLengthMismatch {
        expected: usize,
        actual: usize,
    },
}

fn packed_len(len: usize) -> Result<u32, usize> {
    if len > u32::MAX as usize {
        return Err(len);
    }
    Ok(len as u32)
}

impl From<usize> for Error {
    fn from(len: usize) -> Self {
        Self::ComponentTooLarge { len }
    }
}

impl<S: AsRef<[u8]>> metadata::Metadata for Fields<S> {
    type Iter<'a>
        = Iter<'a>
    where
        S: 'a;

    fn fields(&self) -> Self::Iter<'_> {
        Iter::new(self.bytes.as_ref())
    }
}

impl Fields<Vec<u8>> {
    pub const fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    pub fn try_with(capacity: usize) -> Result<Self, Error> {
        Ok(Self {
            bytes: Vec::try_new(capacity).map_err(Error::Reserve)?,
        })
    }

    pub fn push(&mut self, name: &[u8], value: &[u8]) -> Result<(), Error> {
        self.push_entry(Entry::new(name, value))
    }

    pub fn push_entry(&mut self, field: Entry<'_>) -> Result<(), Error> {
        Space::new(
            &mut self.bytes,
            field.name.len(),
            field.value.len(),
            field.indexing,
        )?
        .generated(field.name, |writer| writer.write_slice(field.value))?
        .commit();
        Ok(())
    }

    /// Publishes only a successful callback that writes exactly `vlen` bytes.
    /// Rejected writes leave the staged value unchanged and may be handled by
    /// the callback; returning their error leaves this field unpublished.
    pub fn push_encoded<F>(&mut self, name: &[u8], vlen: usize, encode: F) -> Result<(), Error>
    where
        F: FnOnce(&mut ValueWriter<'_>) -> Result<(), consume::Error>,
    {
        self.push_with(name, vlen, crate::field::Indexing::Allowed, encode)
    }

    /// Uses the publication and recovery contract of [`Self::push_encoded`].
    pub fn push_with<F>(
        &mut self,
        name: &[u8],
        vlen: usize,
        indexing: crate::field::Indexing,
        encode: F,
    ) -> Result<(), Error>
    where
        F: FnOnce(&mut ValueWriter<'_>) -> Result<(), consume::Error>,
    {
        Space::new(&mut self.bytes, name.len(), vlen, indexing)?
            .generated(name, encode)?
            .commit();
        Ok(())
    }
}

impl<S: AsRef<[u8]>> Fields<S> {
    pub fn iter(&self) -> Iter<'_> {
        Iter::new(self.bytes.as_ref())
    }

    pub fn get(&self, name: &[u8]) -> Option<&[u8]> {
        self.iter()
            .find(|field| field.name == name)
            .map(|field| field.value)
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.as_ref()
    }

    pub fn ranges(&self) -> RangeIter<'_> {
        RangeIter::new(self.bytes.as_ref())
    }
}

impl Default for Fields<Vec<u8>> {
    fn default() -> Self {
        Self::new()
    }
}

/// One callback-local value emission retaining its unpublished owner and exact capacity.
#[repr(transparent)]
pub struct ValueWriter<'a>(write::Staged<'a, usize>);

impl write::Sink for ValueWriter<'_> {
    type Error = consume::Error;

    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0.write_slice(bytes)
    }

    fn write_slices<const N: usize>(&mut self, slices: [&[u8]; N]) -> Result<(), Self::Error> {
        self.0.write_slices(slices)
    }
}

const _: () = {
    assert!(mem::size_of::<OwnedIter<'static>>() == mem::size_of::<&[Owned]>());
    assert!(
        mem::size_of::<ValueWriter<'static>>() == mem::size_of::<write::Staged<'static, usize>>()
    );
};

struct Space<'a> {
    stage: write::Staged<'a, usize>,
    prefix: [u8; Prefix::LEN],
    vlen: usize,
}

impl<'a> Space<'a> {
    fn new(
        bytes: &'a mut Vec<u8>,
        name: usize,
        value: usize,
        indexing: crate::field::Indexing,
    ) -> Result<Self, Error> {
        let nwire = packed_len(name)?;
        let vwire = packed_len(value)?;
        let added = Prefix::LEN + nwire as usize + vwire as usize;
        let stage = write::Staged::<usize>::try_reserve(bytes, added).map_err(Error::Reserve)?;
        Ok(Self {
            stage,
            prefix: Prefix::new(nwire, vwire, indexing).to_bytes(),
            vlen: value,
        })
    }

    fn generated<F>(self, name: &[u8], encode: F) -> Result<write::Staged<'a, usize>, Error>
    where
        F: FnOnce(&mut ValueWriter<'_>) -> Result<(), consume::Error>,
    {
        let expected = self.vlen;
        let prefix = self.stage.capacity() - expected;
        let mut writer = ValueWriter(self.stage);
        writer
            .write_slices([&self.prefix, name])
            .map_err(Error::Write)?;
        encode(&mut writer).map_err(Error::Write)?;
        writer.0.try_complete().map_err(|error| {
            let (_, actual) = error.into_parts();
            Error::ValueLengthMismatch {
                expected,
                actual: actual - prefix,
            }
        })
    }
}
