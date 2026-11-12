use std::{mem::MaybeUninit, num::NonZero};

mod raw;

pub trait Wire {
    fn decimal_len(self) -> NonZero<usize>;

    /// Encodes once backwards, borrowing only the initialized decimal suffix.
    fn decimal(self, buf: &mut [u8; 20]) -> &[u8];

    /// Initializes and borrows only the decimal suffix.
    fn decimal_uninit(self, buf: &mut [MaybeUninit<u8>; 20]) -> &[u8];
}

const _: () = assert!(usize::BITS <= u64::BITS);
