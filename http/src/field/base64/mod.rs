//! Standard Base64 for HTTP field values.

mod decode;
mod decoder;
mod encoded;
mod encoder;
mod shape;

pub use decoder::Decoder;
pub use encoded::Encoded;
pub use encoder::Encoder;

const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Invalid standard Base64 input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Length {
        actual: usize,
    },
    Byte {
        index: usize,
        byte: u8,
    },
    Padding {
        index: usize,
        byte: u8,
    },
    Source {
        start: usize,
        end: usize,
        capacity: usize,
    },
    Output {
        start: usize,
        len: usize,
        capacity: usize,
    },
}

pub(in crate::field::base64) fn value(byte: u8, index: usize) -> Result<u8, Error> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(26 + byte - b'a'),
        b'0'..=b'9' => Ok(52 + byte - b'0'),
        b'+' => Ok(62),
        b'/' => Ok(63),
        invalid => Err(Error::Byte {
            index,
            byte: invalid,
        }),
    }
}

const _: () = {
    assert!(core::mem::size_of::<Error>() == 4 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Result<usize, Error>>() == core::mem::size_of::<Error>());
};
