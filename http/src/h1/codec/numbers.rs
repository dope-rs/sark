use std::num::NonZero;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Overflow {
        current: usize,
        added: NonZero<usize>,
    },
    Capacity {
        required: NonZero<usize>,
        actual: usize,
    },
}

pub trait Wire {
    fn decimal_len(self) -> NonZero<usize>;

    /// Encodes once backwards, borrowing only the initialized decimal suffix.
    fn decimal(self, buf: &mut [u8; 20]) -> &[u8];

    /// Writes the complete decimal at `offset`, returning its exclusive end.
    /// A rejected range leaves the destination unchanged.
    fn try_write(self, buf: &mut [u8], offset: usize) -> Result<NonZero<usize>, Error>;
}

impl Wire for usize {
    fn decimal_len(mut self) -> NonZero<usize> {
        let mut len = NonZero::<usize>::MIN;
        while self >= 10 {
            self /= 10;
            len = len.saturating_add(1);
        }
        len
    }

    fn decimal(mut self, buf: &mut [u8; 20]) -> &[u8] {
        let mut start = buf.len();
        loop {
            start -= 1;
            buf[start] = take(&mut self);
            if self == 0 {
                return &buf[start..];
            }
        }
    }

    fn try_write(self, buf: &mut [u8], offset: usize) -> Result<NonZero<usize>, Error> {
        let width = self.decimal_len();
        let end = width.checked_add(offset).ok_or(Error::Overflow {
            current: offset,
            added: width,
        })?;
        let actual = buf.len();
        let digits = buf.get_mut(offset..end.get()).ok_or(Error::Capacity {
            required: end,
            actual,
        })?;
        let mut value = self;
        for digit in digits.iter_mut().rev() {
            *digit = take(&mut value);
        }
        Ok(end)
    }
}

fn take(value: &mut usize) -> u8 {
    let digit = b'0' + (*value % 10) as u8;
    *value /= 10;
    digit
}

const _: () = assert!(usize::BITS <= u64::BITS);
