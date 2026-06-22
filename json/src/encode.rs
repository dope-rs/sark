use o3::buffer::Owned;

pub struct Encode;

impl Encode {
    pub fn u64_len(mut value: u64) -> usize {
        if value == 0 {
            return 1;
        }
        let mut digits = 0usize;
        while value != 0 {
            digits += 1;
            value /= 10;
        }
        digits
    }

    pub fn str_len(value: &[u8]) -> usize {
        2 + Self::esc_len(value)
    }

    pub fn extend_str(value: &[u8], out: &mut Owned) {
        out.extend_from_slice(b"\"");
        let mut start = 0usize;
        let mut idx = 0usize;
        while idx < value.len() {
            let esc = Self::esc_token(value[idx]);
            if let Some(token) = esc {
                if start != idx {
                    out.extend_from_slice(&value[start..idx]);
                }
                out.extend_from_slice(token.as_bytes());
                idx += 1;
                start = idx;
                continue;
            }
            idx += 1;
        }
        if start != value.len() {
            out.extend_from_slice(&value[start..]);
        }
        out.extend_from_slice(b"\"");
    }

    pub fn extend_u64(out: &mut Owned, value: u64) {
        if value == 0 {
            out.extend_from_slice(b"0");
            return;
        }
        let mut digits = [0u8; 20];
        let mut idx = digits.len();
        let mut value = value;
        while value != 0 {
            idx -= 1;
            digits[idx] = b'0' + (value % 10) as u8;
            value /= 10;
        }
        out.extend_from_slice(&digits[idx..]);
    }

    fn esc_len(value: &[u8]) -> usize {
        let mut len = 0usize;
        let mut idx = 0usize;
        while idx < value.len() {
            len += Self::esc_size(value[idx]);
            idx += 1;
        }
        len
    }

    fn esc_size(byte: u8) -> usize {
        match byte {
            b'"' | b'\\' | 0x08 | b'\n' | 0x0c | b'\r' | b'\t' => 2,
            0x00..=0x1f => 6,
            _ => 1,
        }
    }

    fn esc_token(byte: u8) -> Option<&'static str> {
        match byte {
            b'"' => Some("\\\""),
            b'\\' => Some("\\\\"),
            b'\n' => Some("\\n"),
            b'\r' => Some("\\r"),
            b'\t' => Some("\\t"),
            0x00 => Some("\\u0000"),
            0x01 => Some("\\u0001"),
            0x02 => Some("\\u0002"),
            0x03 => Some("\\u0003"),
            0x04 => Some("\\u0004"),
            0x05 => Some("\\u0005"),
            0x06 => Some("\\u0006"),
            0x07 => Some("\\u0007"),
            0x08 => Some("\\b"),
            0x0b => Some("\\u000b"),
            0x0c => Some("\\f"),
            0x0e => Some("\\u000e"),
            0x0f => Some("\\u000f"),
            0x10 => Some("\\u0010"),
            0x11 => Some("\\u0011"),
            0x12 => Some("\\u0012"),
            0x13 => Some("\\u0013"),
            0x14 => Some("\\u0014"),
            0x15 => Some("\\u0015"),
            0x16 => Some("\\u0016"),
            0x17 => Some("\\u0017"),
            0x18 => Some("\\u0018"),
            0x19 => Some("\\u0019"),
            0x1a => Some("\\u001a"),
            0x1b => Some("\\u001b"),
            0x1c => Some("\\u001c"),
            0x1d => Some("\\u001d"),
            0x1e => Some("\\u001e"),
            0x1f => Some("\\u001f"),
            _ => None,
        }
    }
}
