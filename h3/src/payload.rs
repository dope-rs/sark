use std::fmt;
use std::ops::{Deref, Range};

use o3::buffer::Shared;

enum Storage {
    Unique { bytes: Vec<u8>, range: Range<usize> },
    Shared(Shared),
}

/// An immutable H3 payload that retains its transport allocation.
///
/// A payload occupying the tail of a uniquely owned receive batch keeps the
/// original `Vec` directly. If parsing must split one receive allocation
/// between multiple live views, ownership is promoted to [`Shared`].
pub struct Payload {
    storage: Storage,
}

impl Payload {
    pub(crate) fn from_unique(bytes: Vec<u8>, range: Range<usize>) -> Self {
        assert!(
            range.start <= range.end && range.end <= bytes.len(),
            "H3 payload range is out of bounds"
        );
        Self {
            storage: Storage::Unique { bytes, range },
        }
    }

    pub(crate) fn from_shared(bytes: Shared) -> Self {
        Self {
            storage: Storage::Shared(bytes),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match &self.storage {
            Storage::Unique { bytes, range } => &bytes[range.clone()],
            Storage::Shared(bytes) => bytes.as_slice(),
        }
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    /// Returns a contiguous `Vec`, reusing unique storage when possible.
    ///
    /// A non-zero view offset is compacted in place. Shared storage is copied
    /// because converting a potentially aliased view back to unique ownership
    /// cannot be done safely without proving uniqueness.
    pub fn into_vec(self) -> Vec<u8> {
        let len = self.len();
        self.into_vec_with_capacity(len)
    }

    pub(crate) fn into_vec_with_capacity(self, capacity: usize) -> Vec<u8> {
        match self.storage {
            Storage::Unique { mut bytes, range } => {
                let len = range.len();
                if range.start != 0 {
                    bytes.copy_within(range, 0);
                }
                bytes.truncate(len);
                bytes.reserve_exact(capacity.saturating_sub(len));
                bytes
            }
            Storage::Shared(bytes) => {
                let mut unique = Vec::with_capacity(capacity.max(bytes.len()));
                unique.extend_from_slice(bytes.as_slice());
                unique
            }
        }
    }
}

impl Clone for Payload {
    fn clone(&self) -> Self {
        match &self.storage {
            Storage::Unique { .. } => Self::from(self.as_slice().to_vec()),
            Storage::Shared(bytes) => Self::from_shared(bytes.clone()),
        }
    }
}

impl From<Vec<u8>> for Payload {
    fn from(bytes: Vec<u8>) -> Self {
        let len = bytes.len();
        Self::from_unique(bytes, 0..len)
    }
}

impl Default for Payload {
    fn default() -> Self {
        Self::from(Vec::new())
    }
}

impl From<Shared> for Payload {
    fn from(bytes: Shared) -> Self {
        Self::from_shared(bytes)
    }
}

impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Deref for Payload {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T> PartialEq<T> for Payload
where
    T: AsRef<[u8]> + ?Sized,
{
    fn eq(&self, other: &T) -> bool {
        self.as_slice() == other.as_ref()
    }
}

impl Eq for Payload {}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Payload").field(&self.as_slice()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Payload;

    #[test]
    fn full_unique_payload_round_trips_the_same_vec() {
        let bytes = b"owned".to_vec();
        let allocation = bytes.as_ptr();

        let bytes = Payload::from(bytes).into_vec();

        assert_eq!(bytes, b"owned");
        assert_eq!(bytes.as_ptr(), allocation);
    }

    #[test]
    fn unique_payload_can_grow_in_its_transport_allocation() {
        let mut bytes = Vec::with_capacity(32);
        bytes.extend_from_slice(b"owned");
        let allocation = bytes.as_ptr();

        let bytes = Payload::from(bytes).into_vec_with_capacity(16);

        assert_eq!(bytes, b"owned");
        assert!(bytes.capacity() >= 16);
        assert_eq!(bytes.as_ptr(), allocation);
    }
}
