//! Owners whose shared projections preserve admitted field bytes.

use std::{borrow::Cow, marker::PhantomData};

use o3::{
    alloc::{self, StrExt as _},
    buffer::storage::Shared,
};

pub(in crate::field) mod encoded;
mod lower;
mod name;
mod sealed;
mod value;

pub use lower::Lower;
pub use name::Name;
pub(crate) use sealed::Sealed;
pub use value::Value;

/// Failure to validate and own one field component.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Syntax(crate::field::Error),
    Allocation(alloc::Exhausted),
}

fn try_own(value: Cow<'_, str>) -> Result<String, alloc::Exhausted> {
    match value {
        Cow::Borrowed(value) => value.try_to_owned(),
        Cow::Owned(value) => Ok(value),
    }
}

/// Immutable bytes while their field owner is shared.
pub trait Bytes: Sealed {
    fn as_bytes(&self) -> &[u8];
}

/// Immutable UTF-8 text while its field owner is shared.
pub trait Text: Bytes {
    fn as_str(&self) -> &str;
}

/// One immutable text value selected by its type.
pub trait Constant {
    const VALUE: &'static str;
}

/// A zero-sized owner of a type's immutable text constant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Static<T>(PhantomData<fn() -> T>);

impl<T: Constant> Static<T> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T: Constant> Bytes for Static<T> {
    fn as_bytes(&self) -> &[u8] {
        T::VALUE.as_bytes()
    }
}
impl<T: Constant> Text for Static<T> {
    fn as_str(&self) -> &str {
        T::VALUE
    }
}

impl Bytes for str {
    fn as_bytes(&self) -> &[u8] {
        str::as_bytes(self)
    }
}
impl Text for str {
    fn as_str(&self) -> &str {
        self
    }
}

impl Bytes for String {
    fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}
impl Text for String {
    fn as_str(&self) -> &str {
        String::as_str(self)
    }
}

impl Bytes for [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

impl<const N: usize> Bytes for [u8; N] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

impl Bytes for Vec<u8> {
    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Bytes for Shared {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

impl Bytes for Cow<'_, str> {
    fn as_bytes(&self) -> &[u8] {
        self.as_ref().as_bytes()
    }
}
impl Text for Cow<'_, str> {
    fn as_str(&self) -> &str {
        self.as_ref()
    }
}

impl Bytes for Cow<'_, [u8]> {
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}

impl<T: Bytes + ?Sized> Bytes for &T {
    fn as_bytes(&self) -> &[u8] {
        T::as_bytes(self)
    }
}
impl<T: Text + ?Sized> Text for &T {
    fn as_str(&self) -> &str {
        T::as_str(self)
    }
}

impl<T: Bytes + ?Sized> Bytes for Box<T> {
    fn as_bytes(&self) -> &[u8] {
        T::as_bytes(self)
    }
}
impl<T: Text + ?Sized> Text for Box<T> {
    fn as_str(&self) -> &str {
        T::as_str(self)
    }
}
