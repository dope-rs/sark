use sark_http::request::Target;

#[test]
fn boundaries_match_scalar() {
    for len in [0usize, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65] {
        let valid = vec![b'a'; len];
        assert!(Target::is_valid(&valid));
        for pos in [0, len / 2, len.saturating_sub(1)] {
            if len == 0 {
                continue;
            }
            for &byte in &[0, b'\t', b' ', 0x1f, 0x20, 0x21, 0x7e, 0x7f, 0x80] {
                let mut target = valid.clone();
                target[pos] = byte;
                let scalar = !target.iter().any(|&b| b <= 0x20 || b == 0x7f);
                assert_eq!(
                    Target::is_valid(&target),
                    scalar,
                    "len={len} pos={pos} byte={byte:#x}",
                );
            }
        }
    }
}

#[test]
fn lanes_match_scalar() {
    for len in [16usize, 17, 32, 33, 48, 65, 129] {
        for pos in [0, 7, 15, 16, 23, 31, 32, 47, 48, 63, 64, 127, len - 1] {
            if pos >= len {
                continue;
            }
            for byte in 0u8..=u8::MAX {
                let mut input = vec![b'a'; len];
                input[pos] = byte;
                let scalar = !input.iter().any(|&item| item <= 0x20 || item == 0x7f);
                assert_eq!(
                    Target::is_valid(&input),
                    scalar,
                    "len={len} pos={pos} byte={byte:#x}",
                );
            }
        }
    }
}
