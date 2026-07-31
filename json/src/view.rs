use std::borrow::Cow;

use o3::buffer::Shared;

/// JSON bytes that borrow the request when possible and own only decoded escapes.
#[repr(transparent)]
#[derive(Clone)]
pub struct JsonBytes<'req>(Cow<'req, [u8]>);

impl<'req> JsonBytes<'req> {
    #[must_use]
    pub fn borrowed(bytes: &'req [u8]) -> Self {
        Self(Cow::Borrowed(bytes))
    }

    #[must_use]
    pub fn owned(bytes: Vec<u8>) -> Self {
        Self(Cow::Owned(bytes))
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    #[must_use]
    pub fn into_shared(self) -> Shared {
        match self.0 {
            Cow::Borrowed(bytes) => Shared::copy_from_slice(bytes),
            Cow::Owned(bytes) => Shared::from(bytes),
        }
    }
}

impl AsRef<[u8]> for JsonBytes<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
