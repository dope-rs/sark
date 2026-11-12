use std::{mem::MaybeUninit, num::NonZero, slice};

use super::Wire;

impl Wire for usize {
    fn decimal_len(mut self) -> NonZero<usize> {
        let mut len = NonZero::<usize>::MIN;
        while self >= 10 {
            self /= 10;
            len = len.saturating_add(1);
        }
        len
    }

    fn decimal(self, buf: &mut [u8; 20]) -> &[u8] {
        // SAFETY: MaybeUninit<u8> has the same layout; this exclusive borrow
        // remains initialized because decimal_uninit writes only valid bytes.
        self.decimal_uninit(unsafe { &mut *(buf as *mut [u8; 20]).cast() })
    }

    fn decimal_uninit(mut self, buf: &mut [MaybeUninit<u8>; 20]) -> &[u8] {
        let mut start = buf.len();
        loop {
            start -= 1;
            buf[start].write(b'0' + (self % 10) as u8);
            self /= 10;
            if self == 0 {
                let bytes = &buf[start..];
                // SAFETY: every byte of this suffix was initialized above;
                // usize has at most 20 decimal digits and the borrow is retained.
                return unsafe { slice::from_raw_parts(bytes.as_ptr().cast(), bytes.len()) };
            }
        }
    }
}
