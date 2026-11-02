use std::{slice, str};

use crate::fragment::{Append, Buffer, PrepareError, Utf8};

pub(super) struct Text<'state, 'payload> {
    append: Append<'state, 'payload>,
}

impl<'state, 'payload> Text<'state, 'payload> {
    pub(super) fn try_new(
        buffer: &'state mut Buffer,
        tail: &'payload [u8],
    ) -> Result<Self, PrepareError> {
        validate_pair(&buffer.payload, tail).map_err(PrepareError::Text)?;
        let append = Append::try_new(buffer, tail).map_err(PrepareError::Allocation)?;
        Ok(Self { append })
    }

    pub(super) fn commit(self) -> String {
        // SAFETY: construction proves the joint UTF-8 and keeps both original
        // borrows until joining. No intervening mutation can change the bytes.
        unsafe { String::from_utf8_unchecked(self.append.join()) }
    }
}

fn validate_pair(first: &[u8], second: &[u8]) -> Result<(), Utf8> {
    let base = match str::from_utf8(first) {
        Ok(_) => return Utf8::check(second, first.len()),
        Err(error) if error.error_len().is_some() => return Err(Utf8::from(error)),
        Err(error) => error.valid_up_to(),
    };
    // SAFETY: incomplete UTF-8 proves 1..3 tail bytes and tail.len() < width <= 4.
    // The inputs are disjoint from buf; needed bounds both copied ranges.
    unsafe {
        let tail = first.get_unchecked(base..);
        let needed = (tail.get_unchecked(0).leading_ones() as usize - tail.len()).min(second.len());
        let mut buf = [0u8; 4];
        buf.as_mut_ptr()
            .copy_from_nonoverlapping(tail.as_ptr(), tail.len());
        buf.as_mut_ptr()
            .add(tail.len())
            .copy_from_nonoverlapping(second.as_ptr(), needed);
        Utf8::check(
            slice::from_raw_parts(buf.as_ptr(), tail.len() + needed),
            base,
        )?;
        Utf8::check(second.get_unchecked(needed..), first.len() + needed)
    }
}
