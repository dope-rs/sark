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
