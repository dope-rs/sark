use o3::buffer::{storage::inline, write::Sink as _};

use crate::Error;

#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct Token<const N: usize>(inline::Bytes<N>);

impl<const N: usize> Token<N> {
    pub const fn new() -> Self {
        Self(inline::Bytes::new())
    }

    pub(crate) fn from_slice(bytes: &[u8]) -> crate::Result<Self> {
        inline::Bytes::from_slice(bytes)
            .map(Self)
            .map_err(Error::Token)
    }

    pub(crate) fn push(&mut self, byte: u8) -> crate::Result<()> {
        self.0.write_byte(byte).map_err(Error::Token)
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<const N: usize> Default for Token<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> AsRef<[u8]> for Token<N> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}
