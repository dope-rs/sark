use o3::buffer::{Borrowed, Bytes, Owned, Retained, Shared};

use super::super::{HotTextInner, TextItemInner};
use super::headers::HeaderNameToken;

#[derive(Clone)]
pub enum HeaderValueInner<'req> {
    Static(&'static [u8]),
    Inline(InlineHeaderValue),
    Shared(Shared),
    Borrowed(Bytes<Borrowed<'req>>),
    Retained(Bytes<Retained>),
}

#[derive(Clone, Copy)]
pub struct InlineHeaderValue {
    bytes: [u8; 31],
    len: u8,
}

impl InlineHeaderValue {
    pub fn new(value: &[u8]) -> Self {
        assert!(
            value.len() <= 31,
            "inline header value overflow: max 31, got {}",
            value.len()
        );
        let mut bytes = [0u8; 31];
        bytes[..value.len()].copy_from_slice(value);
        Self {
            bytes,
            len: value.len() as u8,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

impl std::fmt::Debug for InlineHeaderValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InlineHeaderValue")
            .field("len", &self.len)
            .finish()
    }
}

impl<'req> std::fmt::Debug for HeaderValueInner<'req> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static(value) => f
                .debug_struct("HeaderValue::Static")
                .field("len", &value.len())
                .finish(),
            Self::Inline(value) => f
                .debug_struct("HeaderValue::Inline")
                .field("len", &value.as_bytes().len())
                .finish(),
            Self::Shared(value) => f
                .debug_struct("HeaderValue::Shared")
                .field("len", &value.len())
                .finish(),
            Self::Borrowed(value) => f
                .debug_struct("HeaderValue::Borrowed")
                .field("len", &value.len())
                .finish(),
            Self::Retained(value) => f
                .debug_struct("HeaderValue::Retained")
                .field("len", &value.len())
                .finish(),
        }
    }
}

impl<'req> HeaderValueInner<'req> {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Static(value) => value,
            Self::Inline(value) => value.as_bytes(),
            Self::Shared(value) => value.as_ref(),
            Self::Borrowed(value) => value.as_slice(),
            Self::Retained(value) => value.as_slice(),
        }
    }

    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }

    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }
}

#[derive(Clone, Debug)]
pub struct HeaderItemInner<'req> {
    pub(super) name: HeaderNameToken,
    pub(super) value: HeaderValueInner<'req>,
}

pub type HeaderItem = HeaderItemInner<'static>;

impl<'req> HeaderItemInner<'req> {
    pub fn from_value<V>(name: HeaderNameToken, value: V) -> Self
    where
        V: IntoHeaderValue<'req>,
    {
        Self {
            name,
            value: value.into_header_value(),
        }
    }

    pub(crate) fn placeholder() -> Self {
        Self {
            name: HeaderNameToken::empty_placeholder(),
            value: HeaderValueInner::Static(&[]),
        }
    }

    pub fn wire_len(&self) -> usize {
        self.name.as_bytes().len() + 2 + self.value.len() + 2
    }

    pub fn name_bytes(&self) -> &'static [u8] {
        self.name.as_bytes()
    }

    pub fn value_bytes(&self) -> &[u8] {
        self.value.as_bytes()
    }
}

pub trait IntoHeaderValue<'req> {
    fn into_header_value(self) -> HeaderValueInner<'req>;
}

impl<'req> IntoHeaderValue<'req> for HeaderValueInner<'req> {
    fn into_header_value(self) -> Self {
        self
    }
}

impl<'req> IntoHeaderValue<'req> for InlineHeaderValue {
    fn into_header_value(self) -> HeaderValueInner<'req> {
        HeaderValueInner::Inline(self)
    }
}

impl<'req> IntoHeaderValue<'req> for Bytes<Borrowed<'req>> {
    fn into_header_value(self) -> HeaderValueInner<'req> {
        HeaderValueInner::Borrowed(self)
    }
}

impl<'req> IntoHeaderValue<'req> for Bytes<Retained> {
    fn into_header_value(self) -> HeaderValueInner<'req> {
        HeaderValueInner::Retained(self)
    }
}

impl<'req> IntoHeaderValue<'req> for Shared {
    fn into_header_value(self) -> HeaderValueInner<'req> {
        HeaderValueInner::Shared(self)
    }
}

impl<'req> IntoHeaderValue<'req> for Owned {
    fn into_header_value(self) -> HeaderValueInner<'req> {
        HeaderValueInner::Shared(self.freeze())
    }
}

impl<'req> IntoHeaderValue<'req> for String {
    fn into_header_value(self) -> HeaderValueInner<'req> {
        HeaderValueInner::Shared(Shared::from(self.into_bytes()))
    }
}

impl<'req> IntoHeaderValue<'req> for &'static str {
    fn into_header_value(self) -> HeaderValueInner<'req> {
        HeaderValueInner::Static(self.as_bytes())
    }
}

pub trait TextSpec<'req> {
    fn into_hot_text(self) -> HotTextInner<'req>;
}

impl<'req, const N: usize> TextSpec<'req> for [TextItemInner<'req>; N] {
    fn into_hot_text(self) -> HotTextInner<'req> {
        HotTextInner::from_items(self)
    }
}

impl<'req> TextSpec<'req> for HotTextInner<'req> {
    fn into_hot_text(self) -> Self {
        self
    }
}
