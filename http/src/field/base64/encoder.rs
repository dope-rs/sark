use std::convert::Infallible;

use crate::field::{self, base64, storage::encoded};

/// Borrowed bytes ready for standard Base64 encoding.
#[derive(Clone, Copy)]
pub struct Encoder<'a> {
    bytes: &'a [u8],
    padding: Padding,
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum Padding {
    Omit,
    Include,
}

impl<'a> Encoder<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            padding: Padding::Omit,
        }
    }

    pub const fn padded(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            padding: Padding::Include,
        }
    }

    pub fn write(self, mut emit: impl FnMut(&[u8])) {
        match field::Encode::try_write(self, |bytes| {
            emit(bytes);
            Ok::<_, Infallible>(())
        }) {
            Ok(()) => {}
            Err(never) => match never {},
        }
    }
}

impl encoded::Encoded for Encoder<'_> {}

impl field::Encode for Encoder<'_> {
    fn encoded_len(&self) -> usize {
        let blocks = self.bytes.len() / 3;
        let rest = self.bytes.len() % 3;
        let tail = if rest == 0 {
            0
        } else {
            match self.padding {
                Padding::Omit => rest + 1,
                Padding::Include => 4,
            }
        };
        blocks * 4 + tail
    }

    fn try_write<E>(self, mut emit: impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
        let (chunks, rest) = self.bytes.as_chunks::<3>();
        for chunk in chunks {
            let value =
                (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8) | u32::from(chunk[2]);
            let out = [
                base64::TABLE[((value >> 18) & 63) as usize],
                base64::TABLE[((value >> 12) & 63) as usize],
                base64::TABLE[((value >> 6) & 63) as usize],
                base64::TABLE[(value & 63) as usize],
            ];
            emit(&out)?;
        }
        let Some(&first) = rest.first() else {
            return Ok(());
        };
        let Some(&second) = rest.get(1) else {
            let value = u32::from(first) << 16;
            let out = [
                base64::TABLE[((value >> 18) & 63) as usize],
                base64::TABLE[((value >> 12) & 63) as usize],
                b'=',
                b'=',
            ];
            let len = match self.padding {
                Padding::Omit => 2,
                Padding::Include => 4,
            };
            return emit(&out[..len]);
        };
        let value = (u32::from(first) << 16) | (u32::from(second) << 8);
        let out = [
            base64::TABLE[((value >> 18) & 63) as usize],
            base64::TABLE[((value >> 12) & 63) as usize],
            base64::TABLE[((value >> 6) & 63) as usize],
            b'=',
        ];
        let len = match self.padding {
            Padding::Omit => 3,
            Padding::Include => 4,
        };
        emit(&out[..len])
    }
}

const _: () = {
    assert!(isize::MAX as usize / 3 <= (usize::MAX - 4) / 4);
    assert!(core::mem::size_of::<Padding>() == core::mem::size_of::<bool>());
    assert!(core::mem::size_of::<Encoder<'_>>() == 3 * core::mem::size_of::<usize>());
};
