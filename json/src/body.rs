use crate::Result;
use crate::error::Fail;

#[derive(Clone, Copy)]
pub struct InlineToken<const N: usize> {
    len: u8,
    bytes: [u8; N],
}

impl<const N: usize> InlineToken<N> {
    pub const fn new() -> Self {
        Self {
            len: 0,
            bytes: [0; N],
        }
    }

    pub fn push(&mut self, b: u8) -> Result<()> {
        let idx = self.len as usize;
        if idx >= N {
            return Err(Fail::bad());
        }
        self.bytes[idx] = b;
        self.len += 1;
        Ok(())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<const N: usize> Default for InlineToken<N> {
    fn default() -> Self {
        Self::new()
    }
}
