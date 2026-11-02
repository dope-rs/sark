use std::borrow::Cow;

use o3::alloc::{self, BoxSliceExt as _};

use crate::{
    field::{Error, storage, storage::encoded},
    h1::field,
};

/// An owned or borrowed HTTP field value validated before storage.
#[repr(transparent)]
#[derive(Copy, Debug, PartialEq, Eq)]
pub struct Value<T>(pub(in crate::field) T);

impl<T: Copy> Clone for Value<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: storage::Bytes> Value<T> {
    pub fn new(value: T) -> Result<Self, Error> {
        Error::value(value.as_bytes())?;
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn borrow(&self) -> Value<&[u8]> {
        Value(self.as_bytes())
    }
}

impl<T> Value<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<'a> Value<&'a [u8]> {
    pub const fn try_bytes(value: &'a [u8]) -> Result<Self, Error> {
        match Error::value(value) {
            Ok(()) => Ok(Self(value)),
            Err(cause) => Err(cause),
        }
    }

    pub const fn get(self) -> &'a [u8] {
        self.0
    }
}

impl<'field> Value<Cow<'field, [u8]>> {
    pub fn try_copy(value: &[u8]) -> Result<Self, storage::Error> {
        Error::value(value).map_err(storage::Error::Syntax)?;
        Ok(Self(Cow::Owned(
            Box::try_copy(value)
                .map_err(storage::Error::Allocation)?
                .into_vec(),
        )))
    }

    pub(crate) fn try_entry(field: field::Entry<'_>) -> Result<Self, alloc::Exhausted> {
        Ok(Self(Cow::Owned(Box::try_copy(field.value())?.into_vec())))
    }
}

impl Value<Cow<'_, str>> {
    pub fn try_owned(self) -> Result<Value<String>, alloc::Exhausted> {
        Ok(Value(storage::try_own(self.0)?))
    }
}

impl<T: storage::Text> Value<T> {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<T: storage::Bytes> encoded::Encoded for Value<T> {}
