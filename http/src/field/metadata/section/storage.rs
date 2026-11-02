use core::{marker, mem, ops};
use std::collections;

use o3::{
    base::num::bounded,
    buffer::{bytes::scope, pool, pool::owners, storage, write::consume},
};

use crate::field::{
    self,
    metadata::packed::{Decoded, prefix},
};

const MAX_SLOT: u32 = u16::MAX as u32 - Tag::USER_BASE as u32;

/// A schema slot representable after the reserved metadata tags.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Slot(bounded::U32<0, MAX_SLOT>);

impl Slot {
    pub fn try_usize(value: usize) -> Result<Self, bounded::Overflow<MAX_SLOT>> {
        Ok(Self(bounded::U32::try_usize(value)?))
    }

    pub const fn exact<const VALUE: u32>() -> Self {
        Self(bounded::U32::exact::<VALUE>())
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Tag(u16);

impl Tag {
    pub const PREFIX_LEN: usize = mem::size_of::<u16>() + mem::size_of::<u32>() + 1;

    pub const PATH: Self = Self(0);
    pub const CONTENT_LENGTH: Self = Self(1);
    pub const AUTHORITY: Self = Self(2);
    pub const HOST: Self = Self(3);
    pub const PROTOCOL: Self = Self(4);
    const USER_BASE: u16 = 6;
    pub const USER: Self = Self(Self::USER_BASE);

    pub const fn user(slot: Slot) -> Self {
        Self(slot.get() as u16 + Self::USER_BASE)
    }

    pub const fn user_slot(self) -> Option<Slot> {
        match bounded::U32::new(self.0.wrapping_sub(Self::USER_BASE) as u32) {
            Some(slot) => Some(Slot(slot)),
            None => None,
        }
    }

    pub fn prefix(self, len: usize) -> Option<[u8; Self::PREFIX_LEN]> {
        self.prefix_with(len, field::Indexing::Allowed)
    }

    pub fn prefix_with(
        self,
        len: usize,
        indexing: field::Indexing,
    ) -> Option<[u8; Self::PREFIX_LEN]> {
        if len > u32::MAX as usize {
            return None;
        }
        Some(self.prefix_u32(len as u32, indexing))
    }

    pub(in crate::field::metadata) fn prefix_u32(
        self,
        len: u32,
        indexing: field::Indexing,
    ) -> [u8; Self::PREFIX_LEN] {
        let tag = self.0.to_ne_bytes();
        let len = len.to_ne_bytes();
        [
            tag[0],
            tag[1],
            len[0],
            len[1],
            len[2],
            len[3],
            indexing as u8,
        ]
    }
}

#[doc(hidden)]
pub struct Fields {
    storage: pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
}

impl Fields {
    pub fn as_bytes(&self) -> &[u8] {
        self.storage.as_ref()
    }

    pub fn iter_from(&self, offset: usize) -> Result<Iter<'_>, Fault> {
        let bytes = self.storage.as_ref();
        let Some(bytes) = bytes.get(offset..) else {
            return Err(Fault::Start {
                offset,
                len: bytes.len(),
            });
        };
        Ok(Iter { bytes, offset })
    }
}

#[doc(hidden)]
pub struct Bytes<S = pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>> {
    storage: S,
}

/// Moves immutable field storage into one owner identity without changing its format.
pub trait Scope: AsRef<[u8]> + Sized {
    type Scoped<'id>: AsRef<[u8]> + Into<scope::Owner<'id, storage::Shared>>;

    fn scope<T>(self, use_head: impl for<'id> FnOnce(Self::Scoped<'id>) -> T) -> T;
}

impl Bytes {
    pub fn new(
        storage: pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
    ) -> Self {
        Self { storage }
    }
}

impl Scope for Bytes {
    type Scoped<'id> = Bytes<
        scope::Owner<'id, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>,
    >;

    fn scope<T>(self, use_head: impl for<'id> FnOnce(Self::Scoped<'id>) -> T) -> T {
        scope::Owner::with(self.storage, |storage| use_head(Bytes { storage }))
    }
}

impl<S: AsRef<[u8]>> Bytes<S> {
    pub fn as_bytes(&self) -> &[u8] {
        self.storage.as_ref()
    }
}

impl<S: AsRef<[u8]>> AsRef<[u8]> for Bytes<S> {
    fn as_ref(&self) -> &[u8] {
        self.storage.as_ref()
    }
}

impl From<Bytes> for storage::Shared {
    fn from(value: Bytes) -> Self {
        value.storage.into_shared()
    }
}

impl<'id>
    Bytes<scope::Owner<'id, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>>
{
    pub fn view(&self) -> scope::View<'_, 'id> {
        self.storage.view()
    }

    pub fn into_inner(self) -> Bytes {
        Bytes {
            storage: self.storage.into_inner(),
        }
    }
}

impl<'id>
    From<
        Bytes<
            scope::Owner<'id, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>,
        >,
    > for scope::Owner<'id, storage::Shared>
{
    fn from(
        head: Bytes<
            scope::Owner<'id, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>,
        >,
    ) -> Self {
        head.storage.into_shared()
    }
}

pub struct Iter<'a> {
    bytes: &'a [u8],
    offset: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    Indexing {
        offset: usize,
        value: u8,
    },
    Packed {
        offset: usize,
        cause: prefix::Error,
    },
    Start {
        offset: usize,
        len: usize,
    },
    Record {
        offset: usize,
        required: usize,
        available: usize,
    },
    Overflow {
        offset: usize,
        added: usize,
    },
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<(Tag, ops::Range<usize>, &'a [u8]), Fault>;

    fn next(&mut self) -> Option<Self::Item> {
        (!self.bytes.is_empty()).then(|| {
            let bytes = self.bytes;
            let offset = self.offset;
            let Some((prefix, tail)) = bytes.split_at_checked(Tag::PREFIX_LEN) else {
                return Err(Fault::Record {
                    offset,
                    required: Tag::PREFIX_LEN,
                    available: bytes.len(),
                });
            };
            let tag = Tag(u16::from_ne_bytes([prefix[0], prefix[1]]));
            field::Indexing::from_byte(prefix[6]).ok_or(Fault::Indexing {
                offset,
                value: prefix[6],
            })?;
            let len = u32::from_ne_bytes([prefix[2], prefix[3], prefix[4], prefix[5]]) as usize;
            let start = offset.checked_add(Tag::PREFIX_LEN).ok_or(Fault::Overflow {
                offset,
                added: Tag::PREFIX_LEN,
            })?;
            let end = start.checked_add(len).ok_or(Fault::Overflow {
                offset: start,
                added: len,
            })?;
            let Some((value, rest)) = tail.split_at_checked(len) else {
                return Err(Fault::Record {
                    offset,
                    required: end - offset,
                    available: bytes.len(),
                });
            };
            self.bytes = rest;
            self.offset = end;
            Ok((tag, start..end, value))
        })
    }
}

#[doc(hidden)]
pub trait Format {
    const TAGGED: bool;
}

#[doc(hidden)]
pub enum Raw {}

#[doc(hidden)]
pub enum Tagged {}

impl Format for Raw {
    const TAGGED: bool = false;
}

impl Format for Tagged {
    const TAGGED: bool = true;
}

/// Storage whose complete records belong to this exact block and prefix contract.
#[repr(transparent)]
pub struct Finished<B: Block> {
    storage: pool::Span,
    block: marker::PhantomData<B>,
}

impl<B: Block> Finished<B> {
    pub fn try_from(storage: pool::Span) -> Result<Self, Fault> {
        let bytes = storage.as_ref();
        let Some(rest) = bytes.strip_prefix(B::PREFIX) else {
            return Err(Fault::Record {
                offset: 0,
                required: B::PREFIX.len(),
                available: bytes.len(),
            });
        };
        if B::TAGGED {
            for field in (Iter {
                bytes: rest,
                offset: B::PREFIX.len(),
            }) {
                field?;
            }
        } else {
            let mut tail = rest;
            let mut offset = B::PREFIX.len();
            while !tail.is_empty() {
                let prefix = prefix::Prefix::try_read(tail)
                    .map_err(|cause| Fault::Packed { offset, cause })?;
                let (_, next) = prefix
                    .try_take(tail, prefix.name_len() as usize)
                    .map_err(|cause| Fault::Packed { offset, cause })?;
                offset += tail.len() - next.len();
                tail = next;
            }
        }
        Ok(Self::from_lease(storage))
    }

    pub(in crate::field::metadata) fn from_lease(storage: pool::Span) -> Self {
        Self {
            storage,
            block: marker::PhantomData,
        }
    }

    pub fn into_frozen(self) -> pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen> {
        self.storage.freeze()
    }

    /// Taking mutable storage consumes its completion proof.
    pub fn into_lease(self) -> pool::Span {
        self.storage
    }
}

/// An empty block initialized with its own prefix; only record producers can edit it.
#[repr(transparent)]
pub struct Writer<B: Block> {
    pub(in crate::field::metadata) lease: pool::Span,
    block: marker::PhantomData<B>,
}

impl<B: Block> Writer<B> {
    pub(in crate::field::metadata) fn from_lease(lease: pool::Span) -> Self {
        Self {
            lease,
            block: marker::PhantomData,
        }
    }

    pub fn try_new(mut lease: pool::Span) -> Result<Self, consume::Error> {
        lease.truncate(0);
        lease.try_append(B::PREFIX)?.commit();
        Ok(Self {
            lease,
            block: marker::PhantomData,
        })
    }

    pub fn finish(self) -> Finished<B> {
        Finished::from_lease(self.lease)
    }
}

#[doc(hidden)]
pub trait Block: Sized {
    type Format: Format;
    const TAGGED: bool = <Self::Format as Format>::TAGGED;
    const PREFIX: &'static [u8] = &[];

    fn from(storage: Finished<Self>) -> Self;

    fn into_bytes(self) -> Bytes;
}

impl Block for Fields {
    type Format = Tagged;

    fn from(storage: Finished<Self>) -> Self {
        Self {
            storage: storage.into_frozen(),
        }
    }

    fn into_bytes(self) -> Bytes {
        Bytes::new(self.storage)
    }
}

const _: () = assert!(
    mem::size_of::<Finished<Decoded>>()
        == mem::size_of::<pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>()
);

#[derive(Default)]
#[doc(hidden)]
pub struct Scratch {
    pub(in crate::field::metadata) authority: Vec<u8>,
    pub(in crate::field::metadata) indexing: field::Indexing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum Error {
    Capacity { current: usize, additional: usize },
    Reserve(collections::TryReserveError),
}

pub(in crate::field::metadata) fn try_reserve(buf: &mut Vec<u8>, add: usize) -> Result<(), Error> {
    let current = buf.len();
    if current
        .checked_add(add)
        .is_none_or(|total| total > u32::MAX as usize)
    {
        return Err(Error::Capacity {
            current,
            additional: add,
        });
    }
    buf.try_reserve(add).map_err(Error::Reserve)
}

impl Scratch {
    pub fn clear(&mut self) {
        self.authority.clear();
        self.indexing = field::Indexing::Allowed;
    }

    pub fn authority(&self) -> &[u8] {
        &self.authority
    }

    fn total<const N: usize>(slices: &[&[u8]; N]) -> Result<usize, Error> {
        slices.iter().try_fold(0usize, |current, slice| {
            let add = slice.len();
            current
                .checked_add(add)
                .filter(|total| *total <= u32::MAX as usize)
                .ok_or(Error::Capacity {
                    current,
                    additional: add,
                })
        })
    }

    pub(in crate::field::metadata) fn try_replace<const N: usize>(
        &mut self,
        slices: [&[u8]; N],
    ) -> Result<(), Error> {
        let needed = Scratch::total(&slices)?;
        let buf = &mut self.authority;
        try_reserve(buf, needed.saturating_sub(buf.len()))?;
        buf.clear();
        for slice in slices {
            buf.extend_from_slice(slice);
        }
        Ok(())
    }
}

const _: () = assert!(mem::size_of::<Scratch>() == 4 * mem::size_of::<usize>());
