use core::slice;

use sark_http::field::{Indexing, metadata::packed::prefix::Prefix};

use crate::protocol::section::{
    JOINED, Valid,
    iter::{Fields, Parts, Tail, Value},
    seal,
};

// Construction and every successor retain whole fields from the same immutable
// admitted owner, including its name and joined-format proofs.
impl<'a> Fields<'a> {
    pub(super) fn new(owner: &'a Valid) -> Self {
        let bytes = owner.bytes();
        // SAFETY: Valid owns the complete immutable seal followed by the field
        // sequence. The borrow prevents its frozen storage from being released.
        Self(unsafe {
            slice::from_raw_parts(bytes.as_ptr().add(seal::LEN), bytes.len() - seal::LEN)
        })
    }

    pub(super) fn next(&mut self) -> Option<(&'a [u8], Value<'a>, Indexing)> {
        if self.0.is_empty() {
            return None;
        }
        // SAFETY: construction and each successor retain whole admitted fields.
        // Fold checks the name's reserved bit before copying or rewriting any
        // field and only sets JOINED for the part format it writes itself.
        // Prefix::try_read admitted the indexing byte, copied unchanged.
        let (name, value, joined, indexing) = unsafe {
            let prefix = self.0.as_ptr();
            let name_len = prefix.cast::<u32>().read_unaligned();
            let size = prefix.add(4).cast::<u32>().read_unaligned() as usize;
            let indexing = prefix.add(8).cast::<Indexing>().read();
            let len = (name_len & !JOINED) as usize;
            let name = prefix.add(Prefix::LEN);
            let value = name.add(len);
            let rest = value.add(size);
            let left = self.0.len() - Prefix::LEN - len - size;
            self.0 = slice::from_raw_parts(rest, left);
            (
                slice::from_raw_parts(name, len),
                slice::from_raw_parts(value, size),
                name_len & JOINED != 0,
                indexing,
            )
        };
        Some((
            name,
            if joined {
                Value::Joined(Parts(value))
            } else {
                Value::Plain(value)
            },
            indexing,
        ))
    }
}

// Each input and More successor retain a complete canonical part prefix and its
// payload from the same immutable admitted owner.
impl<'a> Parts<'a> {
    pub(super) fn take(self) -> (&'a [u8], Tail<'a>) {
        let mut value = 0usize;
        let mut shift = 0;
        let mut cursor = self.0.as_ptr();
        // SAFETY: Parts can only come from an admitted JOINED field or its More
        // successor. Part::try_new checks 2 * len + end before emitting a canonical
        // usize-bounded varint. Fold's prefix plus decoded payload fits the
        // encoded part plus its comma, preserving the next source boundary.
        // Last leaves the final unprefixed payload; failures never yield Valid.
        unsafe {
            loop {
                let byte = cursor.read();
                cursor = cursor.add(1);
                value |= usize::from(byte & 0x7f).wrapping_shl(shift);
                if byte & 0x80 == 0 {
                    break;
                }
                shift = shift.wrapping_add(7);
            }
            let len = value >> 1;
            let rest = cursor.add(len);
            let end = self.0.as_ptr().add(self.0.len());
            let left = slice::from_raw_parts(rest, end.offset_from_unsigned(rest));
            let tail = if value & 1 == 0 {
                Tail::More(Self(left))
            } else {
                Tail::Last(left)
            };
            (slice::from_raw_parts(cursor, len), tail)
        }
    }
}
