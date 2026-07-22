use o3::buffer::Shared;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Body {
    bytes: Shared,
}

impl Body {
    pub const fn empty() -> Self {
        Self {
            bytes: Shared::new(),
        }
    }

    pub const fn from_static(bytes: &'static [u8]) -> Self {
        Self {
            bytes: Shared::from_static(bytes),
        }
    }

    pub fn from_shared(bytes: Shared) -> Self {
        Self { bytes }
    }

    pub fn repeat(byte: u8, len: usize) -> Self {
        if len == 0 {
            return Self::empty();
        }
        Self::from_shared(o3::buffer::Owned::filled(len, byte).freeze())
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    pub fn into_shared(self) -> Shared {
        self.bytes
    }
}

impl From<Shared> for Body {
    fn from(bytes: Shared) -> Self {
        Self::from_shared(bytes)
    }
}

impl From<&'static [u8]> for Body {
    fn from(bytes: &'static [u8]) -> Self {
        Self::from_static(bytes)
    }
}

impl<const N: usize> From<&'static [u8; N]> for Body {
    fn from(bytes: &'static [u8; N]) -> Self {
        Self::from_static(bytes)
    }
}

impl AsRef<[u8]> for Body {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
