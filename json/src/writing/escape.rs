use std::num::NonZero;

pub(super) struct Split<'a> {
    pub(super) prefix: &'a [u8],
    pub(super) token: Escape,
}

impl<'a> Split<'a> {
    pub(super) fn escape(value: &mut &'a [u8]) -> Option<Self> {
        let input = *value;
        let mut start = 0;
        for chunk in input.as_chunks::<8>().0 {
            if !crate::bytes::is_plain(u64::from_ne_bytes(*chunk), false, true) {
                break;
            }
            start += 8;
        }
        // SAFETY: start counts complete chunks from this exact input, so it is within input.
        let mut rest = unsafe { input.get_unchecked(start..) }.iter();
        while let Some(&byte) = rest.next() {
            if let Some(token) = Escape::new(byte) {
                let suffix = rest.as_slice();
                // SAFETY: next consumed this input's token, so suffix is shorter than input.
                let prefix = unsafe { input.get_unchecked(..input.len() - suffix.len() - 1) };
                *value = suffix;
                return Some(Self { prefix, token });
            }
        }
        None
    }
}

#[derive(Clone, Copy)]
pub(super) struct Escape(NonZero<u8>);

impl Escape {
    // Short forms precede the remaining control bytes, each encoded as \u00XX.
    const ENCODING: ([u8; 256], [u8; 176]) = {
        let mut table = [0; 256];
        let mut bytes = [0; 176];
        let short = [
            (b'"', b'"'),
            (b'\\', b'\\'),
            (b'\n', b'n'),
            (b'\r', b'r'),
            (b'\t', b't'),
            (8, b'b'),
            (12, b'f'),
        ];
        let mut index = 0;
        while index < short.len() {
            table[short[index].0 as usize] = index as u8 + 1;
            bytes[index * 2] = b'\\';
            bytes[index * 2 + 1] = short[index].1;
            index += 1;
        }
        let mut byte = 0;
        while byte < 32 {
            if table[byte] == 0 {
                table[byte] = index as u8 + 1;
                let start = 14 + (index - 7) * 6;
                bytes[start] = b'\\';
                bytes[start + 1] = b'u';
                bytes[start + 2] = b'0';
                bytes[start + 3] = b'0';
                bytes[start + 4] = b"0123456789abcdef"[byte >> 4];
                bytes[start + 5] = b"0123456789abcdef"[byte & 15];
                index += 1;
            }
            byte += 1;
        }
        (table, bytes)
    };

    fn new(byte: u8) -> Option<Self> {
        NonZero::new(Self::ENCODING.0[usize::from(byte)]).map(Self)
    }

    pub(super) fn overhead(&self) -> usize {
        if self.0.get() <= 7 { 1 } else { 5 }
    }

    pub(super) fn bytes(&self) -> &[u8] {
        let index = usize::from(self.0.get()) - 1;
        let len = self.overhead() + 1;
        let start = if len == 2 {
            index * 2
        } else {
            14 + (index - 7) * 6
        };
        // SAFETY: the private constructor carries an index into the same constant
        // encoding; its first seven entries have two bytes and the rest have six.
        unsafe { Self::ENCODING.1.get_unchecked(start..start + len) }
    }
}
