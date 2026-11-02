use std::mem;

use map::{Owned, record::Entry};
use o3::{
    alloc,
    buffer::{bytes::scope, pool, pool::owners, storage::Shared, write},
};
use packed::{Iter, RangeIter, prefix::Prefix};
use write::{SpareExt as _, edit, spans};

use crate::field::metadata::{self, lower::plain::Error, map, packed, section::storage};

#[derive(Debug)]
#[repr(transparent)]
pub struct Decoded<S = pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>> {
    storage: S,
}

impl Decoded {
    /// Validates arbitrary storage once before exposing infallible iteration.
    pub fn try_from(storage: pool::Span) -> Result<Self, storage::Fault> {
        Ok(<Self as storage::Block>::from(storage::Finished::try_from(
            storage,
        )?))
    }
}

impl From<Decoded> for Shared {
    fn from(value: Decoded) -> Self {
        value.storage.into_shared()
    }
}

impl<S: AsRef<[u8]>> Decoded<S> {
    pub fn iter(&self) -> Iter<'_> {
        Iter::new(self.storage.as_ref())
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.storage.as_ref().is_empty()
    }

    pub fn get(&self, name: &[u8]) -> Option<&[u8]> {
        self.iter()
            .find(|field| field.name == name)
            .map(|field| field.value)
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.storage.as_ref()
    }

    pub fn ranges(&self) -> RangeIter<'_> {
        RangeIter::new(self.storage.as_ref())
    }

    pub fn try_to_owned(&self) -> Result<Vec<Owned>, alloc::Error> {
        let mut fields = Vec::new();
        for field in self.iter() {
            let slot = alloc::Vacant::try_new(&mut fields)?;
            slot.insert(Owned::try_from(field)?);
        }
        Ok(fields)
    }
}

impl metadata::Metadata for Decoded {
    type Iter<'a> = Iter<'a>;

    fn fields(&self) -> Self::Iter<'_> {
        Iter::new(self.storage.as_ref())
    }
}

impl storage::Block for Decoded {
    type Format = storage::Raw;

    fn from(storage: storage::Finished<Self>) -> Self {
        Self {
            storage: storage.into_frozen(),
        }
    }

    fn into_bytes(self) -> storage::Bytes {
        storage::Bytes::new(self.storage)
    }
}

impl<S: AsRef<[u8]>> AsRef<[u8]> for Decoded<S> {
    fn as_ref(&self) -> &[u8] {
        self.storage.as_ref()
    }
}

impl PartialEq for Decoded {
    fn eq(&self, other: &Self) -> bool {
        self.storage.as_ref() == other.storage.as_ref()
    }
}

impl Eq for Decoded {}

impl<'a> IntoIterator for &'a Decoded {
    type Item = Entry<'a>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

const _: () = assert!(
    mem::size_of::<Decoded>()
        == mem::size_of::<pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>()
);
const _: () = assert!(mem::size_of::<Option<Decoded>>() == mem::size_of::<Decoded>());

impl storage::Scope for Decoded {
    type Scoped<'id> = Decoded<
        scope::Owner<'id, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>,
    >;

    fn scope<T>(self, use_head: impl for<'id> FnOnce(Self::Scoped<'id>) -> T) -> T {
        scope::Owner::with(self.storage, |storage| use_head(Decoded { storage }))
    }
}

impl<'id>
    Decoded<scope::Owner<'id, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>>
{
    pub fn view(&self) -> scope::View<'_, 'id> {
        self.storage.view()
    }
}

impl<'id>
    From<
        Decoded<
            scope::Owner<'id, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>,
        >,
    > for scope::Owner<'id, Shared>
{
    fn from(
        head: Decoded<
            scope::Owner<'id, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>,
        >,
    ) -> Self {
        head.storage.into_shared()
    }
}

impl storage::Writer<Decoded> {
    /// Commits a field after its producer supplies the name boundary.
    pub fn try_write<E>(
        &mut self,
        write: impl FnOnce(&mut spans::Writer<'_, '_, false>) -> Result<usize, E>,
    ) -> Result<(), E>
    where
        E: From<Error>,
    {
        self.try_write_as(crate::field::Indexing::Allowed, write)
    }

    pub fn try_write_as<E>(
        &mut self,
        indexing: crate::field::Indexing,
        write: impl FnOnce(&mut spans::Writer<'_, '_, false>) -> Result<usize, E>,
    ) -> Result<(), E>
    where
        E: From<Error>,
    {
        let mut output =
            edit::Edit::try_new(self.lease.spare(), &[0; Prefix::LEN]).map_err(Error::Write)?;
        let mut payload = output.bounded();
        let name = write(&mut payload)?;
        payload.commit();
        let payload = output.len() - Prefix::LEN;
        let value = payload
            .checked_sub(name)
            .ok_or(Error::Layout([0, name, payload]))?;
        *output.prefix() = Prefix::try_new(name, value, indexing)?.to_bytes();
        output.commit();
        Ok(())
    }
}
