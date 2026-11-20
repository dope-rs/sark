use std::{borrow::Cow, marker::PhantomData};

use o3::alloc::{self, BoxSliceExt as _};

use crate::{
    field::{self as http, Error, Name, storage},
    h1::field,
    payload::response::headers,
};

/// An immutable lowercase HTTP field name admitted for outgoing protocols.
#[repr(transparent)]
#[derive(Debug, PartialEq, Eq)]
pub struct Lower<T, Scope>(T, PhantomData<fn() -> Scope>);

impl<T: Copy, Scope> Copy for Lower<T, Scope> {}

impl<T: Copy, Scope> Clone for Lower<T, Scope> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: storage::Bytes> http::Lower<T> {
    pub fn new(name: T) -> Result<Self, Error> {
        http::Lower::try_bytes(name.as_bytes())?;
        Ok(Self::admitted(name))
    }
}

impl<T: storage::Bytes, Scope> Lower<T, Scope> {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn borrow(&self) -> Lower<&[u8], Scope> {
        Lower::admitted(self.0.as_bytes())
    }
}

impl<T: storage::Text, Scope> Lower<T, Scope> {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<T, Scope> Lower<T, Scope> {
    const fn admitted(owner: T) -> Self {
        Self(owner, PhantomData)
    }
}

impl<T> http::Lower<T> {
    pub(in crate::field) const fn from_token(owner: T) -> Self {
        Self::admitted(owner)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<'a> http::Lower<&'a [u8]> {
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
        Ok(Self::admitted(name))
    }

    const fn is_managed(self) -> bool {
        matches!(
            self.0,
            b"date"
                | b"server"
                | b"content-length"
                | b"connection"
                | b"transfer-encoding"
                | b"keep-alive"
                | b"proxy-connection"
                | b"upgrade"
                | b"te"
        )
    }
}

impl<'a, Scope> Lower<&'a [u8], Scope> {
    pub const fn get(self) -> &'a [u8] {
        self.0
    }
}

impl<'a> http::Lower<Cow<'a, [u8]>> {
    pub const fn borrowed(name: http::Lower<&'a [u8]>) -> Self {
        Self::admitted(Cow::Borrowed(name.0))
    }
}

impl<'a> TryFrom<Name<&'a [u8]>> for http::Lower<Cow<'a, [u8]>> {
    type Error = alloc::Exhausted;

    fn try_from(name: Name<&'a [u8]>) -> Result<Self, Self::Error> {
        let bytes = name.0;
        if !bytes.iter().any(u8::is_ascii_uppercase) {
            return Ok(Self::admitted(Cow::Borrowed(bytes)));
        }
        let mut bytes = Box::try_copy(bytes)?.into_vec();
        bytes.make_ascii_lowercase();
        Ok(Self::admitted(Cow::Owned(bytes)))
    }
}

impl<'a, Scope> Lower<Cow<'a, str>, Scope> {
    pub const CONTENT_TYPE: Self = Self::admitted(Cow::Borrowed(http::head::known::CONTENT_TYPE));
}

impl<'a> http::Lower<Cow<'a, str>> {
    pub fn try_name(name: Name<Cow<'a, str>>) -> Result<Self, alloc::Exhausted> {
        Ok(Self::admitted(name.try_lower()?.0))
    }

    pub(crate) fn try_entry(entry: field::Entry<'_>) -> Result<Self, alloc::Exhausted> {
        Ok(Self::admitted(Name::try_entry(entry)?.0))
    }

    pub fn try_copy(name: &[u8]) -> Result<Self, storage::Error> {
        Ok(Self::admitted(Name::try_copy(name)?.0))
    }
}

impl<T: storage::Bytes, Scope> AsRef<[u8]> for Lower<T, Scope> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<T: storage::Bytes> headers::Name<T> {
    pub fn new(name: http::Lower<T>) -> Result<Self, headers::Error> {
        if name.borrow().is_managed() {
            return Err(headers::Error::Managed);
        }
        Ok(Self::admitted(name.0))
    }
}

impl<T: storage::Text> AsRef<str> for headers::Name<T> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<T> headers::Name<T> {
    pub fn into_inner(self) -> http::Lower<T> {
        http::Lower::admitted(self.0)
    }
}

impl<'a> headers::Name<&'a [u8]> {
    pub const fn try_bytes(name: &'a [u8]) -> Result<Self, headers::Error> {
        let name = match http::Lower::try_bytes(name) {
            Ok(name) => name,
            Err(error) => return Err(headers::Error::Syntax(error)),
        };
        if name.is_managed() {
            return Err(headers::Error::Managed);
        }
        Ok(Self::admitted(name.0))
    }
}
