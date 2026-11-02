use sark_http::field::scan::Outcome;

pub fn name(bytes: &[u8], start: usize) -> Outcome<(usize, u8)> {
    let mut idx = start;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b == b':' || b == b'\r' {
            return Outcome::Found((idx, b));
        }
        let valid = b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            );
        if !valid {
            return Outcome::Invalid {
                offset: idx,
                actual: b,
            };
        }
        idx += 1;
    }
    Outcome::Partial
}

pub fn value(bytes: &[u8], start: usize) -> Outcome<usize> {
    let mut idx = start;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if byte == b'\r' {
            if idx + 1 == bytes.len() {
                return Outcome::Partial;
            }
            return if bytes[idx + 1] == b'\n' {
                Outcome::Found(idx)
            } else {
                Outcome::Invalid {
                    offset: idx + 1,
                    actual: bytes[idx + 1],
                }
            };
        }
        if (byte < 0x20 && byte != b'\t') || byte == 0x7f {
            return Outcome::Invalid {
                offset: idx,
                actual: byte,
            };
        }
        idx += 1;
    }
    Outcome::Partial
}
