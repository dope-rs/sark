use sark_core::simd::{HeaderNameOutcome, scan_header_name};

fn naive(bytes: &[u8], start: usize) -> HeaderNameOutcome {
    let mut idx = start;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b == b':' || b == b'\r' {
            return HeaderNameOutcome::Found { pos: idx, byte: b };
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
            return HeaderNameOutcome::Invalid;
        }
        idx += 1;
    }
    HeaderNameOutcome::None
}

#[test]
fn matches_naive_with_terminator_at_each_position() {
    let name = b"Content-Type-Header-Long-Enough-To-Span-Several-Simd-Lanes";
    for &byte in &[b':', b'\r', b' ', b'\t', b'@', b'/', 0u8, 0x80] {
        for pos in 0..name.len() {
            let mut v = name.to_vec();
            v[pos] = byte;
            for &start in &[0usize, 1, 7, 15, 16, 17, 31] {
                assert_eq!(
                    scan_header_name(&v, start),
                    naive(&v, start),
                    "byte={byte:#x} pos={pos} start={start}",
                );
            }
        }
    }
}

#[test]
fn matches_naive_at_lane_boundaries() {
    for len in [0usize, 1, 15, 16, 17, 31, 32, 33, 48, 64, 65] {
        let all_valid = vec![b'a'; len];
        assert_eq!(
            scan_header_name(&all_valid, 0),
            naive(&all_valid, 0),
            "all-valid len={len}",
        );
        for pos in [0, len / 2, len.saturating_sub(1)] {
            if len == 0 {
                continue;
            }
            for &byte in b":\r " {
                let mut v = all_valid.clone();
                v[pos] = byte;
                assert_eq!(
                    scan_header_name(&v, 0),
                    naive(&v, 0),
                    "len={len} pos={pos} byte={byte:#x}",
                );
            }
        }
    }
}
