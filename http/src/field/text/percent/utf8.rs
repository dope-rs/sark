use std::{mem, str};

pub(super) enum Utf8 {
    Ready,
    Two(u8),
    ThreeFirst(u8),
    ThreeSecond([u8; 2]),
    FourFirst(u8),
    FourSecond([u8; 2]),
    FourThird([u8; 3]),
}

impl Utf8 {
    pub(super) const fn new() -> Self {
        Self::Ready
    }

    pub(super) const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    pub(super) fn push(&mut self, out: &mut [u8], written: &mut usize, byte: u8) {
        let state = mem::replace(self, Self::Ready);
        match state {
            Self::Ready => self.start(out, written, byte),
            Self::Two(first) => {
                if Self::is_tail(byte) {
                    Self::complete(out, written, [first, byte])
                } else {
                    self.restart(out, written, 1, byte)
                }
            }
            Self::ThreeFirst(first) => {
                if Self::is_tail(byte) {
                    *self = Self::ThreeSecond([first, byte]);
                } else {
                    self.restart(out, written, 1, byte)
                }
            }
            Self::ThreeSecond([first, second]) => {
                if Self::is_tail(byte) {
                    Self::complete(out, written, [first, second, byte])
                } else {
                    self.restart(out, written, 2, byte)
                }
            }
            Self::FourFirst(first) => {
                if Self::is_tail(byte) {
                    *self = Self::FourSecond([first, byte]);
                } else {
                    self.restart(out, written, 1, byte)
                }
            }
            Self::FourSecond([first, second]) => {
                if Self::is_tail(byte) {
                    *self = Self::FourThird([first, second, byte]);
                } else {
                    self.restart(out, written, 2, byte)
                }
            }
            Self::FourThird([first, second, third]) => {
                if Self::is_tail(byte) {
                    Self::complete(out, written, [first, second, third, byte])
                } else {
                    self.restart(out, written, 3, byte)
                }
            }
        }
    }

    pub(super) fn finish(self, out: &mut [u8], written: &mut usize) {
        let pending = match self {
            Self::Ready => 0,
            Self::Two(_) | Self::ThreeFirst(_) | Self::FourFirst(_) => 1,
            Self::ThreeSecond(_) | Self::FourSecond(_) => 2,
            Self::FourThird(_) => 3,
        };
        Self::reject(out, written, pending)
    }

    fn start(&mut self, out: &mut [u8], written: &mut usize, byte: u8) {
        match byte {
            byte if byte.is_ascii() => Self::emit(out, written, byte),
            0xc2..=0xdf => {
                *self = Self::Two(byte);
            }
            0xe0..=0xef => {
                *self = Self::ThreeFirst(byte);
            }
            0xf0..=0xf4 => {
                *self = Self::FourFirst(byte);
            }
            _ => Self::emit(out, written, b'?'),
        }
    }

    fn restart(&mut self, out: &mut [u8], written: &mut usize, len: usize, byte: u8) {
        Self::reject(out, written, len);
        self.start(out, written, byte)
    }

    fn complete<const N: usize>(out: &mut [u8], written: &mut usize, bytes: [u8; N]) {
        if str::from_utf8(&bytes).is_ok() {
            for byte in bytes {
                Self::emit(out, written, byte);
            }
        } else {
            Self::reject(out, written, N)
        }
    }

    fn reject(out: &mut [u8], written: &mut usize, len: usize) {
        for _ in 0..len {
            Self::emit(out, written, b'?');
        }
    }

    fn emit(out: &mut [u8], written: &mut usize, byte: u8) {
        // SAFETY: `Decoder::new` admitted a window that starts at or before the
        // read cursor, and at most one byte is emitted per consumed input byte,
        // so the write cursor never overtakes the read cursor and stays inside
        // that window.
        unsafe { *out.get_unchecked_mut(*written) = byte };
        *written += 1;
    }

    const fn is_tail(byte: u8) -> bool {
        matches!(byte, 0x80..=0xbf)
    }
}

const _: () = assert!(size_of::<Utf8>() <= 6);
