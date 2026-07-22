use o3::buffer::{Borrowed, Bytes, Owned, Retained, Shared};

use super::TextBody;

#[derive(Clone)]
pub enum Body<'req> {
    Owned(Vec<u8>),
    Shared(Shared),
    Borrowed(Bytes<Borrowed<'req>>),
    Retained(Bytes<Retained>),
    StaticSlice(&'static [u8]),
}

impl<'req> std::fmt::Debug for Body<'req> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Owned(buf) => f
                .debug_struct("Body::Owned")
                .field("len", &buf.len())
                .finish(),
            Self::Shared(buf) => f
                .debug_struct("Body::Shared")
                .field("len", &buf.len())
                .finish(),
            Self::Borrowed(buf) => f
                .debug_struct("Body::Borrowed")
                .field("len", &buf.len())
                .finish(),
            Self::Retained(buf) => f
                .debug_struct("Body::Retained")
                .field("len", &buf.len())
                .finish(),
            Self::StaticSlice(buf) => f
                .debug_struct("Body::StaticSlice")
                .field("len", &buf.len())
                .finish(),
        }
    }
}

impl<'req> Body<'req> {
    pub(crate) fn empty() -> Self {
        Self::Owned(Vec::new())
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Owned(buf) => buf.len(),
            Self::Shared(buf) => buf.len(),
            Self::Borrowed(buf) => buf.len(),
            Self::Retained(buf) => buf.len(),
            Self::StaticSlice(buf) => buf.len(),
        }
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Owned(buf) => buf.as_ref(),
            Self::Shared(buf) => buf.as_ref(),
            Self::Borrowed(buf) => buf.as_slice(),
            Self::Retained(buf) => buf.as_slice(),
            Self::StaticSlice(buf) => buf,
        }
    }

    pub(crate) fn is_shared(&self) -> bool {
        matches!(
            self,
            Self::Shared(_) | Self::Borrowed(_) | Self::Retained(_) | Self::StaticSlice(_)
        )
    }

    pub(crate) fn into_bytes(self) -> Shared {
        match self {
            Self::Owned(buf) => Shared::from(buf),
            Self::Shared(buf) => buf,
            Self::Borrowed(buf) => Shared::copy_from_slice(buf.as_slice()),
            Self::Retained(buf) => buf.into_shared(),
            Self::StaticSlice(buf) => Shared::from_static(buf),
        }
    }

    pub(crate) fn into_owned(self) -> Vec<u8> {
        match self {
            Self::Owned(buf) => buf,
            Self::Shared(buf) => buf.as_ref().to_vec(),
            Self::Borrowed(buf) => buf.as_slice().to_vec(),
            Self::Retained(buf) => buf.as_slice().to_vec(),
            Self::StaticSlice(buf) => buf.to_vec(),
        }
    }

    pub(crate) fn as_owned_mut(&mut self) -> &mut Vec<u8> {
        if matches!(
            self,
            Self::Shared(_) | Self::Borrowed(_) | Self::Retained(_) | Self::StaticSlice(_)
        ) {
            let old = std::mem::replace(self, Self::empty());
            *self = Self::Owned(old.into_owned());
        }
        match self {
            Self::Owned(buf) => buf,
            _ => unreachable!("response body must be owned after conversion"),
        }
    }
}

impl<'req> From<Owned> for Body<'req> {
    fn from(body: Owned) -> Self {
        Self::Shared(body.freeze())
    }
}

impl<'req> From<Shared> for Body<'req> {
    fn from(body: Shared) -> Self {
        Self::Shared(body)
    }
}

impl<'req> From<Bytes<Borrowed<'req>>> for Body<'req> {
    fn from(body: Bytes<Borrowed<'req>>) -> Self {
        Self::Borrowed(body)
    }
}

impl<'req> From<Bytes<Retained>> for Body<'req> {
    fn from(body: Bytes<Retained>) -> Self {
        Self::Retained(body)
    }
}

impl<'req> From<TextBody<'req>> for Body<'req> {
    fn from(body: TextBody<'req>) -> Self {
        Self::Shared(body.into_bytes())
    }
}

impl<'req> From<Vec<u8>> for Body<'req> {
    fn from(body: Vec<u8>) -> Self {
        Self::Owned(body)
    }
}

impl<'req> From<String> for Body<'req> {
    fn from(body: String) -> Self {
        Self::Owned(body.into_bytes())
    }
}

impl<'req> From<&[u8]> for Body<'req> {
    fn from(body: &[u8]) -> Self {
        Self::Owned(body.to_vec())
    }
}

impl<'req> From<&str> for Body<'req> {
    fn from(body: &str) -> Self {
        Self::Owned(body.as_bytes().to_vec())
    }
}
