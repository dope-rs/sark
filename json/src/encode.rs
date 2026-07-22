pub trait Write {
    fn put(&mut self, src: &[u8]);

    fn put_str(&mut self, value: &[u8]) {
        self.put(b"\"");
        let mut start = 0usize;
        let mut idx = 0usize;
        while idx < value.len() {
            if let Some(token) = Encode::esc_token(value[idx]) {
                if start != idx {
                    self.put(&value[start..idx]);
                }
                self.put(token.as_bytes());
                idx += 1;
                start = idx;
                continue;
            }
            idx += 1;
        }
        if start != value.len() {
            self.put(&value[start..]);
        }
        self.put(b"\"");
    }

    fn put_str_plain(&mut self, value: &[u8]) {
        self.put(b"\"");
        self.put(value);
        self.put(b"\"");
    }

    fn put_u64(&mut self, value: u64) {
        if value == 0 {
            self.put(b"0");
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
        self.put(&digits[idx..]);
    }

    fn put_i64(&mut self, value: i64) {
        if value < 0 {
            self.put(b"-");
        }
        self.put_u64(value.unsigned_abs());
    }

    fn put_f64(&mut self, value: f64) {
        if value.is_finite() {
            self.put(ryu::Buffer::new().format_finite(value).as_bytes());
        } else {
            self.put(b"null");
        }
    }
}

pub struct Writer<'a> {
    inner: &'a mut Vec<u8>,
    start: usize,
}

impl<'a> Writer<'a> {
    pub fn new(buf: &'a mut Vec<u8>, estimate: usize) -> Self {
        let start = buf.len();
        buf.reserve(estimate);
        Self { inner: buf, start }
    }

    pub fn finish(self) -> usize {
        self.inner.len() - self.start
    }
}

impl Write for Writer<'_> {
    fn put(&mut self, src: &[u8]) {
        self.inner.extend_from_slice(src);
    }
}

impl Write for o3::buffer::Owned {
    fn put(&mut self, src: &[u8]) {
        self.extend_from_slice(src);
    }
}

pub(crate) struct SliceWriter<'a> {
    out: &'a mut [u8],
    len: usize,
}

impl<'a> SliceWriter<'a> {
    pub(crate) fn new(out: &'a mut [u8]) -> Self {
        Self { out, len: 0 }
    }

    pub(crate) fn finish(self) -> usize {
        self.len
    }
}

impl Write for SliceWriter<'_> {
    fn put(&mut self, src: &[u8]) {
        let end = self
            .len
            .checked_add(src.len())
            .expect("JSON output length overflow");
        self.out
            .get_mut(self.len..end)
            .expect("JsonEncode wrote beyond json_len")
            .copy_from_slice(src);
        self.len = end;
    }
}

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

    pub fn i64_len(value: i64) -> usize {
        Self::u64_len(value.unsigned_abs()) + usize::from(value < 0)
    }

    pub fn f64_len(value: f64) -> usize {
        if value.is_finite() {
            ryu::Buffer::new().format_finite(value).len()
        } else {
            4
        }
    }

    pub fn str_len(value: &[u8]) -> usize {
        2 + Self::esc_len(value)
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
        match Self::esc_token(byte) {
            Some(token) => token.len(),
            None => 1,
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
