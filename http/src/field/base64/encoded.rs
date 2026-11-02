use crate::field::base64::{
    self, Error,
    shape::{Shape, Tail},
};

const LANES: usize = 16;

/// Borrowed standard Base64 text.
#[derive(Clone, Copy)]
pub struct Encoded<'a>(&'a [u8]);

impl<'a> Encoded<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    pub fn decoded_len(self) -> Result<usize, Error> {
        Shape::new(self.0).map(Shape::decoded)
    }

    pub fn validate(self) -> Result<usize, Error> {
        let shape = self.shape()?;
        Ok(shape.decoded())
    }

    pub(super) fn shape(self) -> Result<Shape, Error> {
        let shape = Shape::new(self.0)?;
        let (checked, mask, padding) = match shape.tail() {
            Tail::None => (shape.symbols(), 0, 0),
            Tail::Two => (shape.groups() * 4 + 1, 0x0f, 1),
            Tail::Three => (shape.groups() * 4 + 2, 0x03, 1),
        };
        let blocks = checked / LANES;
        self.0
            .as_chunks::<LANES>()
            .0
            .iter()
            .take(blocks)
            .enumerate()
            .try_for_each(|(block, bytes)| Self::check_chunk(bytes, block * LANES))?;
        let index = blocks * LANES;
        self.0
            .iter()
            .copied()
            .enumerate()
            .skip(index)
            .take(checked - index + padding)
            .try_for_each(|(index, byte)| {
                if index < checked {
                    return Self::check(byte, index);
                }
                if base64::value(byte, index)? & mask != 0 {
                    return Err(Error::Padding { index, byte });
                }
                Ok(())
            })?;
        Ok(shape)
    }

    fn check_chunk(bytes: &[u8; LANES], index: usize) -> Result<(), Error> {
        let valid = bytes.iter().fold(true, |valid, &byte| {
            valid
                & ((byte.wrapping_sub(b'A') <= 25)
                    | (byte.wrapping_sub(b'a') <= 25)
                    | (byte.wrapping_sub(b'0') <= 9)
                    | (byte == b'+')
                    | (byte == b'/'))
        });
        if valid {
            return Ok(());
        }
        bytes
            .iter()
            .copied()
            .enumerate()
            .try_for_each(|(offset, byte)| Self::check(byte, index + offset))
    }

    fn check(byte: u8, index: usize) -> Result<(), Error> {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'+' | b'/' => Ok(()),
            invalid => Err(Error::Byte {
                index,
                byte: invalid,
            }),
        }
    }
}
