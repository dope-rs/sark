use crate::field::text::percent::{Error, scan};

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct Length(u32);

impl Length {
    pub const ZERO: Self = Self(0);

    pub(super) fn measure(mut bytes: &[u8]) -> Result<Self, Error> {
        let maximum = u32::MAX as usize;
        if bytes.len() > maximum {
            return Err(Error::Input {
                actual: bytes.len(),
            });
        }
        let mut length = bytes.len();
        while !bytes.is_empty() {
            let (_, suffix) = scan::Scan::new(bytes).plain();
            let Some((_, tail)) = suffix.split_first() else {
                break;
            };
            if length > maximum - 2 {
                return Err(Error::Escape { current: length });
            }
            length += 2;
            bytes = tail;
        }
        Ok(Self(length as u32))
    }

    pub const fn get(self) -> usize {
        self.0 as usize
    }
}
