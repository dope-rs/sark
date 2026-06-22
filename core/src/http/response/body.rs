use o3::buffer::{Owned, Shared};

use super::TextBody;
use crate::http::LocalFrameBytesRef;

#[derive(Clone)]
pub enum BodyInner<'req> {
    Owned(Owned),
    Shared(Shared),
    Local(LocalFrameBytesRef<'req>),
    StaticSlice(&'static [u8]),
}

pub type Body = BodyInner<'static>;

impl<'req> std::fmt::Debug for BodyInner<'req> {
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
            Self::Local(buf) => f
                .debug_struct("Body::Local")
                .field("len", &buf.len())
                .finish(),
            Self::StaticSlice(buf) => f
                .debug_struct("Body::StaticSlice")
                .field("len", &buf.len())
                .finish(),
        }
    }
}

impl<'req> BodyInner<'req> {
    pub(crate) fn empty() -> Self {
        Self::Owned(Owned::new())
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Owned(buf) => buf.len(),
            Self::Shared(buf) => buf.len(),
            Self::Local(buf) => buf.len(),
            Self::StaticSlice(buf) => buf.len(),
        }
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Owned(buf) => buf.as_ref(),
            Self::Shared(buf) => buf.as_ref(),
            Self::Local(buf) => buf.as_bytes(),
            Self::StaticSlice(buf) => buf,
        }
    }

    pub(crate) fn is_shared(&self) -> bool {
        matches!(
            self,
            Self::Shared(_) | Self::Local(_) | Self::StaticSlice(_)
        )
    }

    pub(crate) fn into_bytes(self) -> Shared {
        match self {
            Self::Owned(buf) => buf.freeze(),
            Self::Shared(buf) => buf,
            Self::Local(buf) => buf.into_bytes(),
            Self::StaticSlice(buf) => Shared::from_static(buf),
        }
    }

    pub(crate) fn into_owned(self) -> Owned {
        match self {
            Self::Owned(buf) => buf,
            Self::Shared(buf) => Owned::from(buf.as_ref()),
            Self::Local(buf) => Owned::from(buf.as_bytes()),
            Self::StaticSlice(buf) => Owned::from(buf),
        }
    }

    pub(crate) fn as_owned_mut(&mut self) -> &mut Owned {
        if matches!(
            self,
            Self::Shared(_) | Self::Local(_) | Self::StaticSlice(_)
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

pub trait IntoBody<'req>: sealed::SealedBody {
    fn into_response_body(self) -> BodyInner<'req>;
}

impl<'req> IntoBody<'req> for BodyInner<'req> {
    fn into_response_body(self) -> Self {
        self
    }
}

impl<'req> IntoBody<'req> for Owned {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Owned(self)
    }
}

impl<'req> IntoBody<'req> for Shared {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Shared(self)
    }
}

impl<'req> IntoBody<'req> for LocalFrameBytesRef<'req> {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Local(self)
    }
}

impl<'req> IntoBody<'req> for TextBody {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Shared(self.into_bytes())
    }
}

impl<'req> IntoBody<'req> for Vec<u8> {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Owned(Owned::from(self.as_slice()))
    }
}

impl<'req> IntoBody<'req> for String {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Owned(Owned::from(self.as_bytes()))
    }
}

impl<'req> IntoBody<'req> for &[u8] {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Owned(Owned::from(self))
    }
}

impl<'req> IntoBody<'req> for &str {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Owned(Owned::from(self.as_bytes()))
    }
}

mod sealed {
    use super::super::{LocalFrameBytesRef, TextBody};
    use super::BodyInner;

    pub trait SealedBody {}

    impl<'req> SealedBody for BodyInner<'req> {}
    impl SealedBody for TextBody {}
    impl SealedBody for o3::buffer::Owned {}
    impl SealedBody for o3::buffer::Shared {}
    impl<'req> SealedBody for LocalFrameBytesRef<'req> {}
    impl SealedBody for Vec<u8> {}
    impl SealedBody for String {}
    impl SealedBody for &[u8] {}
    impl SealedBody for &str {}
}
