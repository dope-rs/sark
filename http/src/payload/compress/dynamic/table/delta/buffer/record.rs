use crate::{
    field::metadata::block,
    payload::compress::dynamic::{
        ENTRY_OVERHEAD, Field, Whole, field, hash,
        table::{delta::Admission, state::Size},
    },
};

#[derive(Clone, Copy)]
#[repr(C)]
pub(super) struct Record {
    digest: hash::Digest,
    start: u32,
    name_end: u32,
    end: u32,
    validation: Whole,
}

pub(super) struct Plan<'a> {
    field: Field<'a>,
    record: Record,
    ordinal: u32,
    next: u32,
}

impl<'a> Plan<'a> {
    /// Admits `len < entries <= u32::MAX`, so `len + 1` fits u32.
    /// Admits `used + name <= required <= bytes <= u32::MAX`, so `used + name` fits u32.
    pub(super) fn new(
        field: Field<'a>,
        digest: hash::Digest,
        size: Size,
        used: u32,
        len: u32,
        bytes: u32,
        entries: u32,
    ) -> Result<Self, Admission> {
        let name = field.name.len();
        let limit = field::NAME_LEN_MASK as usize;
        if name > limit {
            return Err(Admission::Name {
                actual: name,
                limit,
            });
        }
        if len >= entries {
            return Err(Admission::Entries {
                len: len as usize,
                limit: entries as usize,
            });
        }
        let required = (used as usize)
            .checked_add(size.data_len())
            .ok_or(Admission::Length {
                name,
                value: field.value.len(),
            })?;
        let limit = bytes as usize;
        if required > limit {
            return Err(Admission::Bytes { required, limit });
        }
        Ok(Self {
            field,
            record: Record {
                digest,
                start: used,
                name_end: used + name as u32,
                end: required as u32,
                validation: field.validation,
            },
            ordinal: len,
            next: len + 1,
        })
    }

    pub(super) const fn field(&self) -> Field<'a> {
        self.field
    }

    pub(super) const fn record(&self) -> Record {
        self.record
    }

    pub(super) const fn ordinal(&self) -> usize {
        self.ordinal as usize
    }

    pub(super) const fn raw_ordinal(&self) -> u32 {
        self.ordinal
    }

    pub(super) const fn next(&self) -> u32 {
        self.next
    }
}

impl Record {
    pub(super) const fn name_end(self) -> usize {
        self.name_end as usize
    }

    pub(super) const fn proof(self) -> block::Name {
        self.validation.name()
    }

    pub(super) const fn validation(self) -> Whole {
        self.validation
    }

    pub(super) const fn start(self) -> usize {
        self.start as usize
    }

    pub(super) const fn end(self) -> usize {
        self.end as usize
    }

    pub(super) const fn digest(self) -> hash::Digest {
        self.digest
    }
}

const _: () = assert!(core::mem::size_of::<Record>() == ENTRY_OVERHEAD);
