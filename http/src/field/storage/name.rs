use std::borrow::Cow;

use o3::alloc::{self, StrExt as _};

use crate::{
    field::{self, Error, head::known::Known, storage},
    h1::field::Entry,
};

/// An owned or borrowed HTTP field name validated before storage.
#[repr(transparent)]
#[derive(Copy, Debug, PartialEq, Eq)]
pub struct Name<T>(pub(in crate::field) T);

impl<T: Copy> Clone for Name<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: storage::Bytes> Name<T> {
    pub fn new(name: T) -> Result<Self, Error> {
        Error::name(name.as_bytes())?;
        Ok(Self(name))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn as_str(&self) -> &str {
        field::raw::name(self.borrow())
    }

    pub fn borrow(&self) -> Name<&[u8]> {
        Name(self.as_bytes())
    }
}

impl<T> Name<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<'a> Name<&'a [u8]> {
    pub const fn try_bytes(name: &'a [u8]) -> Result<Self, Error> {
        match Error::name(name) {
            Ok(()) => Ok(Self(name)),
            Err(cause) => Err(cause),
        }
    }

    pub const fn get(self) -> &'a [u8] {
        self.0
    }
}

impl<'field> Name<Cow<'field, str>> {
    pub const CONTENT_TYPE: Self = Self(Cow::Borrowed(Known::ContentType.as_str()));

    pub fn try_copy(name: &[u8]) -> Result<Self, storage::Error> {
        if let Some(known) = Known::from_bytes(name) {
            return Ok(Self(Cow::from(known)));
        }
        let name = Name::try_bytes(name).map_err(storage::Error::Syntax)?;
        Self::try_lowered(name).map_err(storage::Error::Allocation)
    }

    pub(crate) fn try_entry(field: Entry<'_>) -> Result<Self, alloc::Exhausted> {
        let name = field.name();
        match Known::from_bytes(name) {
            Some(known) => Ok(Self(Cow::from(known))),
            None => Self::try_lowered(Name(name)),
        }
    }

    fn try_lowered(name: Name<&[u8]>) -> Result<Self, alloc::Exhausted> {
        let mut name = name.as_str().try_to_owned()?;
        name.make_ascii_lowercase();
        Ok(Self(Cow::Owned(name)))
    }

    pub fn try_owned(self) -> Result<Name<String>, alloc::Exhausted> {
        Ok(Name(storage::try_own(self.0)?))
    }

    pub(crate) fn try_lower(mut self) -> Result<Self, alloc::Exhausted> {
        if self.0.bytes().any(|byte| byte.is_ascii_uppercase()) {
            match Known::from_bytes(self.0.as_bytes()) {
                Some(known) => self.0 = Cow::from(known),
                None => {
                    let mut name = storage::try_own(self.0)?;
                    name.make_ascii_lowercase();
                    self.0 = Cow::Owned(name);
                }
            }
        }
        Ok(self)
    }
}
