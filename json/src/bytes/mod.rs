use std::borrow;

pub(crate) mod proofs;
pub(crate) mod seals;

use o3::{buffer::storage, mem::Rejected};

/// Admits words without escapes, optionally checking ASCII and control bytes.
/// Cross-byte borrows only mark lanes after a matching byte.
#[inline]
pub(crate) fn is_plain(word: u64, ascii: bool, control: bool) -> bool {
    const HIGH: u64 = 0x8080_8080_8080_8080;
    const ONES: u64 = 0x0101_0101_0101_0101;
    let quote = word ^ u64::from_ne_bytes([b'"'; 8]);
    let slash = word ^ u64::from_ne_bytes([b'\\'; 8]);
    let control = if control {
        word.wrapping_sub(u64::from_ne_bytes([0x20; 8])) & !word
    } else {
        0
    };
    let quote = quote.wrapping_sub(ONES) & !quote;
    let slash = slash.wrapping_sub(ONES) & !slash;
    (control | quote | slash | if ascii { word } else { 0 }) & HIGH == 0
}

/// JSON bytes that borrow the request when possible and own only decoded escapes.
#[repr(transparent)]
#[derive(Debug)]
pub struct Bytes<'req>(borrow::Cow<'req, [u8]>);

impl<'req> Bytes<'req> {
    #[must_use]
    pub fn borrowed(bytes: &'req [u8]) -> Self {
        Self(borrow::Cow::Borrowed(bytes))
    }

    #[must_use]
    pub fn owned(bytes: Vec<u8>) -> Self {
        Self(borrow::Cow::Owned(bytes))
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

    /// Transfers owned bytes or copies a borrowed view, returning the input on failure.
    pub fn try_share(mut self) -> Result<storage::Shared, Rejected<Self, storage::AllocError>> {
        let result = match &mut self.0 {
            borrow::Cow::Borrowed(bytes) => storage::Shared::try_copy(bytes),
            borrow::Cow::Owned(bytes) => {
                storage::Shared::try_from(bytes).map_err(storage::AllocError::Exhausted)
            }
        };
        result.map_err(|error| Rejected::new(self, error))
    }
}

impl AsRef<[u8]> for Bytes<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
