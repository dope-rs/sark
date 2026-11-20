use std::{mem::MaybeUninit, num::NonZero, slice};

use super::Wire;

pub const PAIRS: [[u8; 2]; 100] = {
    let mut pairs = [[0; 2]; 100];
    let mut value = 0;
    while value < pairs.len() {
        pairs[value] = [b'0' + (value / 10) as u8, b'0' + (value % 10) as u8];
        value += 1;
    }
    pairs
};

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
        while self >= 10 {
            start -= 1;
            buf[start].write(b'0' + (self % 10) as u8);
            self /= 10;
        }
        start -= 1;
        buf[start].write(b'0' + self as u8);
        let bytes = &buf[start..];
        // SAFETY: every byte of this suffix was initialized above;
        // usize has at most 20 decimal digits and the borrow is retained.
        unsafe { slice::from_raw_parts(bytes.as_ptr().cast(), bytes.len()) }
    }
}
