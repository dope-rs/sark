use o3::buffer::Shared;

pub struct Wire;

impl Wire {
    pub const fn decimal_len(mut n: usize) -> usize {
        let mut len = 1;
        while n >= 10 {
            n /= 10;
            len += 1;
        }
        len
    }

    pub fn write_dec(n: usize, buf: &mut [u8]) -> usize {
        let len = Self::decimal_len(n);
        let mut n = n;
        let mut i = len;
        loop {
            i -= 1;
            buf[i] = b'0' + (n % 10) as u8;
            n /= 10;
            if i == 0 {
                return len;
            }
        }
    }

    pub fn write_hex(n: usize, buf: &mut [u8; 16]) -> usize {
        if n == 0 {
            buf[0] = b'0';
            return 1;
        }
        let mut i = 16;
        let mut val = n;
        while val > 0 {
            i -= 1;
            buf[i] = b"0123456789abcdef"[val & 0xf];
            val >>= 4;
        }
        buf.copy_within(i..16, 0);
        16 - i
    }

    pub fn chunk_prefix(size: usize) -> ([u8; 18], usize) {
        let mut hex = [0u8; 16];
        let hex_len = Self::write_hex(size, &mut hex);
        let mut out = [0u8; 18];
        out[..hex_len].copy_from_slice(&hex[..hex_len]);
        out[hex_len] = b'\r';
        out[hex_len + 1] = b'\n';
        (out, hex_len + 2)
    }

    pub fn chunk_frame(body: Shared) -> Shared {
        let (prefix, prefix_len) = Self::chunk_prefix(body.len());
        let capacity = prefix_len
            .checked_add(body.len())
            .and_then(|len| len.checked_add(2))
            .expect("chunk frame length overflow");
        let mut framed = o3::buffer::Owned::with_capacity(capacity);
        framed.extend_from_slice(&prefix[..prefix_len]);
        framed.extend_from_slice(&body);
        framed.extend_from_slice(b"\r\n");
        framed.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::Wire;

    #[test]
    fn decimal_writer_matches_usize_display() {
        for value in [0, 1, 9, 10, 99, 100, 999, 1_000, usize::MAX] {
            let expected = value.to_string();
            let mut out = [0u8; 20];
            let written = Wire::write_dec(value, &mut out);
            assert_eq!(written, Wire::decimal_len(value));
            assert_eq!(&out[..written], expected.as_bytes());
        }
    }
}
