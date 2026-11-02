use crate::{
    field,
    field::{
        storage::encoded,
        text::percent::{Error, Length, octet, scan},
    },
};

pub struct Encoder<'a> {
    text: &'a str,
    length: Length,
}

impl<'a> Encoder<'a> {
    pub fn new(text: &'a str) -> Result<Self, Error> {
        Ok(Self {
            text,
            length: Length::measure(text.as_bytes())?,
        })
    }

    /// Reuses the encoded length admitted for this exact immutable text.
    /// # Safety
    /// `length` must describe these same text bytes, measured by this codec.
    pub const unsafe fn from_length(text: &'a str, length: Length) -> Self {
        Self { text, length }
    }

    pub const fn length(&self) -> Length {
        self.length
    }
}

impl encoded::Encoded for Encoder<'_> {}

impl field::Encode for Encoder<'_> {
    fn encoded_len(&self) -> usize {
        self.length.get()
    }

    fn try_write<E>(self, mut emit: impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
        let mut bytes = self.text.as_bytes();
        while !bytes.is_empty() {
            let (literal, suffix) = scan::Scan::new(bytes).plain();
            emit(literal)?;
            let Some((&byte, tail)) = suffix.split_first() else {
                return Ok(());
            };
            emit(&octet::encode(byte))?;
            bytes = tail;
        }
        Ok(())
    }
}

const _: () = assert!(core::mem::size_of::<Encoder<'_>>() == 3 * core::mem::size_of::<usize>());
