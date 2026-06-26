pub fn is_ascii_ws(b: u8) -> bool {
    b == b' ' || b == b'\t'
}

pub(super) fn trim_ws_range(bytes: &[u8], start: usize, end: usize) -> (usize, usize) {
    let mut vs = start;
    let mut ve = end;
    while vs < ve && is_ascii_ws(bytes[vs]) {
        vs += 1;
    }
    while ve > vs && is_ascii_ws(bytes[ve - 1]) {
        ve -= 1;
    }
    (vs, ve)
}

pub(super) fn is_forbidden_value_byte(b: u8) -> bool {
    (b < 0x20 && b != b'\t') || b == 0x7f
}

pub(super) fn value_has_forbidden_byte(bytes: &[u8]) -> bool {
    const ONES: u64 = 0x0101_0101_0101_0101;
    const HIGH: u64 = 0x8080_8080_8080_8080;
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let x = u64::from_le_bytes(chunk.try_into().unwrap());
        let lt_ctl = x.wrapping_sub(ONES * 0x20) & !x & HIGH;
        let del = {
            let v = x ^ (ONES * 0x7f);
            v.wrapping_sub(ONES) & !v & HIGH
        };
        if (lt_ctl | del) != 0 && chunk.iter().any(|&b| is_forbidden_value_byte(b)) {
            return true;
        }
    }
    chunks
        .remainder()
        .iter()
        .any(|&b| is_forbidden_value_byte(b))
}

pub fn is_header_name_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric()
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
        )
}
