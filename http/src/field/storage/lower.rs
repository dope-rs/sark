use std::borrow::Cow;

use o3::alloc::{self, BoxSliceExt as _};

use crate::{
    field::{Error, Name, storage},
    h1::field,
};

/// An immutable lowercase HTTP field name admitted for outgoing protocols.
#[repr(transparent)]
#[derive(Copy, Debug, PartialEq, Eq)]
pub struct Lower<T>(pub(in crate::field) T);

impl<T: Copy> Clone for Lower<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: storage::Bytes> Lower<T> {
    pub fn new(name: T) -> Result<Self, Error> {
        Lower::try_bytes(name.as_bytes())?;
        Ok(Self(name))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn borrow(&self) -> Lower<&[u8]> {
        Lower(self.0.as_bytes())
    }
}

impl<T: storage::Text> Lower<T> {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<T> Lower<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<'a> Lower<&'a [u8]> {
    pub const fn try_bytes(name: &'a [u8]) -> Result<Self, Error> {
        if name.is_empty() {
            return Err(Error::Empty);
        }
        let mut index = 0;
        while index < name.len() {
            let byte = name[index];
            if !Error::is_name_byte(byte) || byte.is_ascii_uppercase() {
                return Err(Error::InvalidByte { index, byte });
            }
            index += 1;
        }
        Ok(Self(name))
    }

    pub const fn get(self) -> &'a [u8] {
        self.0
    }
}

impl<'a> Lower<Cow<'a, [u8]>> {
    pub const fn borrowed(name: Lower<&'a [u8]>) -> Self {
        Self(Cow::Borrowed(name.0))
    }
}

impl<'a> TryFrom<Name<&'a [u8]>> for Lower<Cow<'a, [u8]>> {
    type Error = alloc::Exhausted;

    fn try_from(name: Name<&'a [u8]>) -> Result<Self, Self::Error> {
        let bytes = name.0;
        if !bytes.iter().any(u8::is_ascii_uppercase) {
            return Ok(Self(Cow::Borrowed(bytes)));
        }
        let mut bytes = Box::try_copy(bytes)?.into_vec();
        bytes.make_ascii_lowercase();
        Ok(Self(Cow::Owned(bytes)))
    }
}

impl<'a> Lower<Cow<'a, str>> {
    pub const CONTENT_TYPE: Self = Self(Cow::Borrowed("content-type"));

    pub fn try_name(name: Name<Cow<'a, str>>) -> Result<Self, alloc::Exhausted> {
        Ok(Self(name.try_lower()?.0))
    }

    pub(crate) fn try_entry(entry: field::Entry<'_>) -> Result<Self, alloc::Exhausted> {
        Ok(Self(Name::try_entry(entry)?.0))
    }

    pub fn try_copy(name: &[u8]) -> Result<Self, storage::Error> {
        Ok(Self(Name::try_copy(name)?.0))
    }
}

impl<T: storage::Bytes> AsRef<[u8]> for Lower<T> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}
