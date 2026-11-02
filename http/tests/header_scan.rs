pub mod common;

use common::wire::scan;
use sark_http::field::scan::Bytes;

#[test]
fn name_matches_reference() {
    let name = b"Content-Type-Header-Long-Enough-To-Span-Several-Simd-Lanes";
    for &byte in &[b':', b'\r', b' ', b'\t', b'@', b'/', 0u8, 0x80] {
        for pos in 0..name.len() {
            let mut v = name.to_vec();
            v[pos] = byte;
            for &start in &[0usize, 1, 7, 15, 16, 17, 31] {
                assert!(
                    (Bytes::new(&v).name(start)) == (scan::name(&v, start)),
                    "byte={byte:#x} pos={pos} start={start}"
                );
            }
        }
    }
}

#[test]
fn name_lane_boundaries() {
    for len in [0usize, 1, 15, 16, 17, 31, 32, 33, 48, 64, 65] {
        let valid = vec![b'a'; len];
        assert!(
            (Bytes::new(&valid).name(0)) == (scan::name(&valid, 0)),
            "all-valid len={len}"
        );
        for pos in [0, len / 2, len.saturating_sub(1)] {
            if len == 0 {
                continue;
            }
            for &byte in b":\r " {
                let mut v = valid.clone();
                v[pos] = byte;
                assert!(
                    (Bytes::new(&v).name(0)) == (scan::name(&v, 0)),
                    "len={len} pos={pos} byte={byte:#x}"
                );
            }
        }
    }
}

#[test]
fn name_all_bytes_in_lanes() {
    for len in [17usize, 32, 33, 48, 65, 80, 81, 128, 129] {
        for pos in [0, 7, 15, 16, 23, 31, 32, 47, 48, 63, len - 1] {
            if pos >= len {
                continue;
            }
            for byte in 0u8..=u8::MAX {
                let mut input = vec![b'a'; len];
                input[pos] = byte;
                input.push(b':');
                assert!(
                    (Bytes::new(&input).name(0)) == (scan::name(&input, 0)),
                    "len={len} pos={pos} byte={byte:#x}"
                );
            }
        }
    }
}

#[test]
fn value_matches_reference() {
    for len in [0usize, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 81, 145] {
        let visible = vec![b'a'; len];
        for &start in &[0usize, 1, 7, 15, 16, 17, 31] {
            assert!(
                (Bytes::new(&visible).value(start)) == (scan::value(&visible, start)),
                "visible len={len} start={start}"
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
                assert!(
                    (Bytes::new(&value).value(0)) == (scan::value(&value, 0)),
                    "len={len} pos={pos} byte={byte:#x}"
                );
            }
        }
    }
}

#[test]
fn value_all_bytes_in_lanes() {
    for len in [17usize, 32, 33, 48, 65, 81, 145] {
        for pos in [
            0,
            7,
            15,
            16,
            23,
            31,
            32,
            47,
            48,
            63,
            79,
            80,
            143,
            144,
            len - 1,
        ] {
            if pos >= len {
                continue;
            }
            for byte in 0u8..=u8::MAX {
                let mut input = vec![b'a'; len];
                input[pos] = byte;
                input.extend_from_slice(b"\r\n");
                assert!(
                    (Bytes::new(&input).value(0)) == (scan::value(&input, 0)),
                    "len={len} pos={pos} byte={byte:#x}"
                );
            }
        }
    }
}
