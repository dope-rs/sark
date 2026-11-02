use core::mem;

mod raw;

/// A WebSocket masking key stored in native word order.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Key(u32);

/// The four-byte masking phase carried across payload chunks.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Cursor(Key);

impl Key {
    pub const fn new(bytes: [u8; 4]) -> Self {
        Self(u32::from_ne_bytes(bytes))
    }

    pub const fn bytes(self) -> [u8; 4] {
        self.0.to_ne_bytes()
    }

    pub const fn cursor(self) -> Cursor {
        Cursor(self)
    }

    pub const fn advanced(self, offset: usize) -> Self {
        let word = u32::from_le_bytes(self.bytes());
        Self::new(
            word.rotate_right((offset as u32).wrapping_mul(8))
                .to_le_bytes(),
        )
    }

    pub(crate) fn initializer(self, source: &[u8]) -> raw::Init<'_> {
        raw::Init::new(self, source)
    }
}

impl Cursor {
    pub const fn key(self) -> Key {
        self.0
    }

    pub fn apply<'buf>(&mut self, buf: &'buf mut [u8]) -> &'buf mut [u8] {
        let next = self.0.advanced(buf.len());
        let output = self.0.apply(buf);
        self.0 = next;
        output
    }
}

const _: () = assert!(mem::size_of::<Key>() == 4);
const _: () = assert!(mem::size_of::<Cursor>() == 4);
