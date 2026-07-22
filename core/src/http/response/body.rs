use o3::buffer::{Borrowed, Bytes, Retained, Shared};

use super::TextBody;

#[derive(Clone)]
pub enum BodyInner<'req> {
    Owned(Vec<u8>),
    Shared(Shared),
    Borrowed(Bytes<Borrowed<'req>>),
    Retained(Bytes<Retained>),
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

impl<'req> BodyInner<'req> {
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

    pub(super) fn into_static(self) -> Body {
        match self {
            Self::Owned(buf) => BodyInner::Owned(buf),
            Self::Shared(buf) => BodyInner::Shared(buf),
            Self::Borrowed(buf) => BodyInner::Shared(Shared::copy_from_slice(buf.as_slice())),
            Self::Retained(buf) => BodyInner::Retained(buf),
            Self::StaticSlice(buf) => BodyInner::StaticSlice(buf),
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

pub trait IntoBody<'req>: sealed::SealedBody {
    fn into_response_body(self) -> BodyInner<'req>;
}

impl<'req> IntoBody<'req> for BodyInner<'req> {
    fn into_response_body(self) -> Self {
        self
    }
}

impl<'req> IntoBody<'req> for o3::buffer::Owned {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Shared(self.freeze())
    }
}

impl<'req> IntoBody<'req> for Shared {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Shared(self)
    }
}

impl<'req> IntoBody<'req> for Bytes<Borrowed<'req>> {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Borrowed(self)
    }
}

impl<'req> IntoBody<'req> for Bytes<Retained> {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Retained(self)
    }
}

impl<'req> IntoBody<'req> for TextBody {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Shared(self.into_bytes())
    }
}

impl<'req> IntoBody<'req> for Vec<u8> {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Owned(self)
    }
}

impl<'req> IntoBody<'req> for String {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Owned(self.into_bytes())
    }
}

impl<'req> IntoBody<'req> for &[u8] {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Owned(self.to_vec())
    }
}

impl<'req> IntoBody<'req> for &str {
    fn into_response_body(self) -> BodyInner<'req> {
        BodyInner::Owned(self.as_bytes().to_vec())
    }
}

mod sealed {
    use super::super::TextBody;
    use super::BodyInner;

    pub trait SealedBody {}

    impl<'req> SealedBody for BodyInner<'req> {}
    impl SealedBody for TextBody {}
    impl SealedBody for o3::buffer::Owned {}
    impl SealedBody for o3::buffer::Shared {}
    impl SealedBody for o3::buffer::Bytes<o3::buffer::Borrowed<'_>> {}
    impl SealedBody for o3::buffer::Bytes<o3::buffer::Retained> {}
    impl SealedBody for Vec<u8> {}
    impl SealedBody for String {}
    impl SealedBody for &[u8] {}
    impl SealedBody for &str {}
}
