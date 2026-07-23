use sark_core::http::scan::{
    HeaderNameOutcome, HeaderValueOutcome, request_target_is_valid, scan_header_name,
    scan_header_value,
};

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

fn naive_value(bytes: &[u8], start: usize) -> HeaderValueOutcome {
    let mut idx = start;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if byte == b'\r' {
            if idx + 1 == bytes.len() {
                return HeaderValueOutcome::None;
            }
            return if bytes[idx + 1] == b'\n' {
                HeaderValueOutcome::Found { pos: idx }
            } else {
                HeaderValueOutcome::Invalid
            };
        }
        if (byte < 0x20 && byte != b'\t') || byte == 0x7f {
            return HeaderValueOutcome::Invalid;
        }
        idx += 1;
    }
    HeaderValueOutcome::None
}

#[test]
fn value_scan_matches_naive_across_simd_boundaries() {
    for len in [0usize, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65] {
        let visible = vec![b'a'; len];
        for &start in &[0usize, 1, 7, 15, 16, 17, 31] {
            assert_eq!(
                scan_header_value(&visible, start),
                naive_value(&visible, start),
                "visible len={len} start={start}",
            );
        }
        for pos in [0, len / 2, len.saturating_sub(1)] {
            if len == 0 {
                continue;
            }
            for &byte in &[b'\t', b'\n', b'\r', 0, 0x1f, 0x7f, 0x80] {
                let mut value = visible.clone();
                value[pos] = byte;
                if byte == b'\r' && pos + 1 < len {
                    value[pos + 1] = b'\n';
                }
                assert_eq!(
                    scan_header_value(&value, 0),
                    naive_value(&value, 0),
                    "len={len} pos={pos} byte={byte:#x}",
                );
            }
        }
    }
}

#[test]
fn request_target_validation_matches_scalar_across_simd_boundaries() {
    for len in [0usize, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65] {
        let valid = vec![b'a'; len];
        assert!(request_target_is_valid(&valid));
        for pos in [0, len / 2, len.saturating_sub(1)] {
            if len == 0 {
                continue;
            }
            for &byte in &[0, b'\t', b' ', 0x1f, 0x20, 0x21, 0x7e, 0x7f, 0x80] {
                let mut target = valid.clone();
                target[pos] = byte;
                let scalar = !target.iter().any(|&b| b <= 0x20 || b == 0x7f);
                assert_eq!(
                    request_target_is_valid(&target),
                    scalar,
                    "len={len} pos={pos} byte={byte:#x}",
                );
            }
        }
    }
}
