use std::borrow::Cow;

use crate::{
    field::{self, storage},
    payload::response::headers::Error,
};

/// An ordinary lowercase application header outside framework-managed names.
#[derive(Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Name<T>(pub(in crate::payload::response::headers) field::Lower<T>);

impl<T: Copy> Clone for Name<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: storage::Bytes> Name<T> {
    pub fn new(name: field::Lower<T>) -> Result<Self, Error> {
        if Name::<&[u8]>::is_managed(name.as_bytes()) {
            return Err(Error::Managed);
        }
        Ok(Self(name))
    }

    pub fn borrow(&self) -> Name<&[u8]> {
        Name(self.0.borrow())
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl<T: storage::Bytes> AsRef<[u8]> for Name<T> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<T: storage::Text> Name<T> {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<T: storage::Text> AsRef<str> for Name<T> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<T> Name<T> {
    pub fn into_inner(self) -> field::Lower<T> {
        self.0
    }
}

impl<'a> Name<&'a [u8]> {
    pub const fn try_bytes(name: &'a [u8]) -> Result<Self, Error> {
        let name = match field::Lower::try_bytes(name) {
            Ok(name) => name,
            Err(error) => return Err(Error::Syntax(error)),
        };
        if Self::is_managed(name.get()) {
            return Err(Error::Managed);
        }
        Ok(Self(name))
    }

    pub const fn is_managed(name: &[u8]) -> bool {
        name.eq_ignore_ascii_case(b"date")
            || name.eq_ignore_ascii_case(b"server")
            || name.eq_ignore_ascii_case(b"content-length")
            || name.eq_ignore_ascii_case(b"connection")
            || name.eq_ignore_ascii_case(b"transfer-encoding")
            || name.eq_ignore_ascii_case(b"keep-alive")
            || name.eq_ignore_ascii_case(b"proxy-connection")
            || name.eq_ignore_ascii_case(b"upgrade")
            || name.eq_ignore_ascii_case(b"te")
    }

    pub const fn get(self) -> &'a [u8] {
        self.0.get()
    }
}

impl Name<Cow<'_, str>> {
    pub const CONTENT_TYPE: Self = Self(field::Lower::CONTENT_TYPE);
}
