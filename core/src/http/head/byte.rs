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

pub fn header_lines(wire: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> {
    wire.split(|&b| b == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
        .take_while(|line| !line.is_empty())
        .filter_map(|line| {
            let pos = line.iter().position(|&b| b == b':')?;
            let (ns, ne) = trim_ws_range(line, 0, pos);
            let (vs, ve) = trim_ws_range(line, pos + 1, line.len());
            Some((&line[ns..ne], &line[vs..ve]))
        })
}
