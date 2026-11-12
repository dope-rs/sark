use std::{ops, str};

use o3::{buffer::storage, mem::Rejected};
use sark_http::request;

use crate::{Bytes, Token, bytes::seals, input::Input as _};

/// Immutable byte owners supported by JSON admission.
#[doc(hidden)]
pub trait Backing: seals::Owner + AsRef<[u8]> {}

impl<T: seals::Owner + AsRef<[u8]>> Backing for T {}

/// An immutable owner whose complete bytes are valid UTF-8.
#[derive(Copy, Debug)]
#[repr(transparent)]
pub struct Text<T> {
    owner: T,
}

/// Valid UTF-8 containing no JSON string escape characters.
#[derive(Copy, Debug)]
#[repr(transparent)]
pub struct Plain<T> {
    text: Text<T>,
}

/// One complete validated JSON value, including its UTF-8 proof.
#[derive(Copy, Debug)]
#[repr(transparent)]
pub struct Raw<T> {
    text: Text<T>,
}

impl<T: Copy> Clone for Text<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Copy> Clone for Plain<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Copy> Clone for Raw<T> {
    fn clone(&self) -> Self {
        *self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlainError {
    Utf8(str::Utf8Error),
    Escape { index: usize, byte: u8 },
}

impl<T: Backing> Text<T> {
    pub fn try_new(owner: T) -> Result<Self, Rejected<T, str::Utf8Error>> {
        if let Err(error) = str::from_utf8(owner.as_ref()) {
            return Err(Rejected::new(owner, error));
        }
        Ok(Self::admitted(owner))
    }
}

impl<T: Backing> Plain<T> {
    pub fn try_new(owner: T) -> Result<Self, Rejected<T, PlainError>> {
        if let Err(error) = str::from_utf8(owner.as_ref()) {
            return Err(Rejected::new(owner, PlainError::Utf8(error)));
        }
        for (index, &byte) in owner.as_ref().iter().enumerate() {
            if byte < 0x20 || matches!(byte, b'"' | b'\\') {
                return Err(Rejected::new(owner, PlainError::Escape { index, byte }));
            }
        }
        Ok(Self::admitted(owner))
    }
}

impl<T: Backing> Raw<T> {
    pub fn try_new(owner: T) -> Result<Self, Rejected<T, crate::Error>> {
        let input = owner.as_ref();
        let mut index = 0;
        if let Err(error) = input
            .skip_value(&mut index)
            .and_then(|()| input.finish(&mut index))
        {
            return Err(Rejected::new(owner, error));
        }
        Ok(Self::admitted(owner))
    }
}

impl<T: Backing> TryFrom<Text<T>> for Raw<T> {
    type Error = Rejected<Text<T>, crate::Error>;

    fn try_from(text: Text<T>) -> Result<Self, Self::Error> {
        if let Err(error) = crate::input::try_raw(text.as_str()) {
            return Err(Rejected::new(text, error));
        }
        Ok(Self { text })
    }
}

impl<'a> From<&'a str> for Text<&'a [u8]> {
    fn from(owner: &'a str) -> Self {
        Self::admitted(owner.as_bytes())
    }
}

impl From<String> for Text<String> {
    fn from(owner: String) -> Self {
        Self::admitted(owner)
    }
}

impl From<Token<0>> for Text<Token<0>> {
    fn from(owner: Token<0>) -> Self {
        Self::admitted(owner)
    }
}

impl<T> From<Plain<T>> for Text<T> {
    fn from(value: Plain<T>) -> Self {
        value.text
    }
}

impl<T> From<Raw<T>> for Text<T> {
    fn from(value: Raw<T>) -> Self {
        value.text
    }
}

/// String inputs whose UTF-8 proof survives until the complete write.
pub trait TextValue: seals::Text {
    fn as_bytes(&self) -> &[u8];
}

impl<T: seals::Text + AsRef<[u8]> + ?Sized> TextValue for T {
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}
impl<T: Backing> seals::Text for Text<T> {}
impl<T: Backing> seals::Text for Plain<T> {}

impl<T> Text<T> {
    pub(crate) const fn admitted(owner: T) -> Self {
        Self { owner }
    }
    pub fn into_inner(self) -> T {
        self.owner
    }
}
impl<T> Plain<T> {
    pub(crate) const fn admitted(owner: T) -> Self {
        Self {
            text: Text::admitted(owner),
        }
    }
    pub fn into_inner(self) -> T {
        self.text.owner
    }
}
impl<T> Raw<T> {
    pub(crate) const fn admitted(owner: T) -> Self {
        Self {
            text: Text::admitted(owner),
        }
    }
    pub fn into_inner(self) -> T {
        self.text.owner
    }
}

impl<T: Backing> Text<T> {
    pub fn as_slice(&self) -> &[u8] {
        self.owner.as_ref()
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.owner.as_ref()
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: admission establishes UTF-8 on this complete immutable owner;
        // no API exposes a mutable owner or changes its byte range under this proof.
        unsafe { str::from_utf8_unchecked(self.owner.as_ref()) }
    }
}

impl<T: Backing> AsRef<[u8]> for Text<T> {
    fn as_ref(&self) -> &[u8] {
        self.owner.as_ref()
    }
}
impl<T: Backing> AsRef<[u8]> for Plain<T> {
    fn as_ref(&self) -> &[u8] {
        self.text.owner.as_ref()
    }
}
impl<T: Backing> AsRef<[u8]> for Raw<T> {
    fn as_ref(&self) -> &[u8] {
        self.text.owner.as_ref()
    }
}
impl<T: Backing> ops::Deref for Text<T> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.owner.as_ref()
    }
}
impl<T> ops::Deref for Plain<T> {
    type Target = Text<T>;
    fn deref(&self) -> &Text<T> {
        &self.text
    }
}
impl<T> ops::Deref for Raw<T> {
    type Target = Text<T>;
    fn deref(&self) -> &Text<T> {
        &self.text
    }
}
impl<T: Backing, U: AsRef<[u8]>> PartialEq<U> for Text<T> {
    fn eq(&self, other: &U) -> bool {
        self.owner.as_ref() == other.as_ref()
    }
}
impl<T: Backing, U: AsRef<[u8]>> PartialEq<U> for Plain<T> {
    fn eq(&self, other: &U) -> bool {
        self.text.owner.as_ref() == other.as_ref()
    }
}
impl<T: Backing, U: AsRef<[u8]>> PartialEq<U> for Raw<T> {
    fn eq(&self, other: &U) -> bool {
        self.text.owner.as_ref() == other.as_ref()
    }
}

impl Text<storage::Shared> {
    pub fn try_copy(value: &str) -> Result<Self, storage::AllocError> {
        storage::Shared::try_copy(value.as_bytes()).map(Self::admitted)
    }

    pub fn into_request<'d>(self, id: request::Id<'d>) -> Text<request::Bytes<'d>> {
        Text::admitted(request::Bytes::from_shared(id, self.owner))
    }
}
impl Plain<storage::Shared> {
    pub fn into_request<'d>(self, id: request::Id<'d>) -> Plain<request::Bytes<'d>> {
        Plain {
            text: self.text.into_request(id),
        }
    }
}
impl Raw<storage::Shared> {
    pub fn into_request<'d>(self, id: request::Id<'d>) -> Raw<request::Bytes<'d>> {
        Raw {
            text: self.text.into_request(id),
        }
    }
}

impl Text<Bytes<'_>> {
    pub fn try_share(self) -> Result<Text<storage::Shared>, Rejected<Self, storage::AllocError>> {
        self.owner
            .try_share()
            .map(Text::admitted)
            .map_err(|rejected| rejected.map(Text::admitted))
    }
}
