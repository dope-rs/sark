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
pub struct Decoded<S = pool::Span, const PREFIX: usize = 0> {
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

impl<const PREFIX: usize> Decoded<pool::Span, PREFIX> {
    /// Consumes the record proof and returns its exclusively owned storage.
    pub fn into_lease(self) -> pool::Span {
        self.storage
    }
}

impl<const PREFIX: usize> From<Decoded<pool::Span, PREFIX>> for Shared {
    fn from(value: Decoded<pool::Span, PREFIX>) -> Self {
        value.storage.freeze().into_shared()
    }
}

impl<S: AsRef<[u8]>, const PREFIX: usize> Decoded<S, PREFIX> {
    pub fn iter(&self) -> Iter<'_> {
        Iter::new(&self.storage.as_ref()[PREFIX..])
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.storage.as_ref().len() == PREFIX
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
        RangeIter::new(self.storage.as_ref(), PREFIX)
    }

    pub fn try_to_owned(&self) -> Result<Vec<Owned>, alloc::Error> {
        let mut fields = Vec::new();
        for field in self.iter() {
            alloc::try_vacant(&mut fields)?(Owned::try_from(field)?);
        }
        Ok(fields)
    }
}

impl<const PREFIX: usize> metadata::Metadata for Decoded<pool::Span, PREFIX> {
    type Iter<'a> = Iter<'a>;

    fn fields(&self) -> Self::Iter<'_> {
        self.iter()
    }
}

impl<const PREFIX: usize> storage::Block for Decoded<pool::Span, PREFIX> {
    type Format = storage::Raw;
    const PREFIX: &'static [u8] = &[0; PREFIX];

    fn from(storage: storage::Finished<Self>) -> Self {
        Self {
            storage: storage.into_lease(),
        }
    }

    fn into_bytes(self) -> storage::Bytes {
        storage::Bytes::new(self.storage.freeze())
    }
}

impl<S: AsRef<[u8]>, const PREFIX: usize> AsRef<[u8]> for Decoded<S, PREFIX> {
    fn as_ref(&self) -> &[u8] {
        self.storage.as_ref()
    }
}

impl<const PREFIX: usize> PartialEq for Decoded<pool::Span, PREFIX> {
    fn eq(&self, other: &Self) -> bool {
        self.storage.as_ref() == other.storage.as_ref()
    }
}

impl<const PREFIX: usize> Eq for Decoded<pool::Span, PREFIX> {}

impl<'a, const PREFIX: usize> IntoIterator for &'a Decoded<pool::Span, PREFIX> {
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

impl<const PREFIX: usize> storage::Scope for Decoded<pool::Span, PREFIX> {
    type Scoped<'id> = Decoded<
        scope::Owner<'id, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>,
        PREFIX,
    >;

    fn scope<T>(self, use_head: impl for<'id> FnOnce(Self::Scoped<'id>) -> T) -> T {
        // Freeze before issuing an identity; a scoped or shared owner cannot
        // recover the mutable lease through the unscoped consuming boundary.
        scope::Owner::with(self.storage.freeze(), |storage| {
            use_head(Decoded { storage })
        })
    }
}

impl<'id, const PREFIX: usize>
    Decoded<
        scope::Owner<'id, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>,
        PREFIX,
    >
{
    pub fn view(&self) -> scope::View<'_, 'id> {
        self.storage.view()
    }
}

impl<'id, const PREFIX: usize>
    From<
        Decoded<
            scope::Owner<'id, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>,
            PREFIX,
        >,
    > for scope::Owner<'id, Shared>
{
    fn from(
        head: Decoded<
            scope::Owner<'id, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>,
            PREFIX,
        >,
    ) -> Self {
        head.storage.into_shared()
    }
}

impl<const PREFIX: usize> storage::Writer<Decoded<pool::Span, PREFIX>> {
    /// Commits a field after its producer supplies the name boundary.
    pub fn try_write<E>(
        &mut self,
        write: impl FnOnce(&mut spans::Writer<'_, '_>) -> Result<usize, E>,
    ) -> Result<(), E>
    where
        E: From<Error>,
    {
        self.try_write_as(crate::field::Indexing::Allowed, write)
    }

    pub fn try_write_as<E>(
        &mut self,
        indexing: crate::field::Indexing,
        write: impl FnOnce(&mut spans::Writer<'_, '_>) -> Result<usize, E>,
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
