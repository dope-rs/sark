pub mod common;

use sark_ws::mask;

use crate::common::Mask as _;

#[test]
fn matches_naive_aligned() {
    for &len in &[
        0usize, 1, 3, 4, 7, 15, 16, 17, 31, 32, 33, 64, 65, 1024, 4096,
    ] {
        let data: Vec<u8> = (0u8..=255).cycle().take(len).collect();
        let mask = [0xCA, 0xFE, 0xBA, 0xBE];

        let mut a = data.clone();
        let mut b = data.clone();
        let output = mask::Key::new(mask).apply(&mut a);
        b.unmask(mask);
        assert_eq!(output, b, "mismatch at len={len}");
    }
}

#[test]
fn round_trip() {
    let mask = [0x12, 0x34, 0x56, 0x78];
    let input: Vec<u8> = (0u8..=255).cycle().take(2003).collect();
    let mut buf = input.clone();
    let masked = mask::Key::new(mask).apply(&mut buf);
    let clear = mask::Key::new(mask).apply(masked);
    assert_eq!(clear, input);
}

#[test]
fn spans_match_naive() {
    const GUARD: u8 = 0xA5;
    let mask = [0x01, 0x80, 0x7f, 0xfe];

    for len in (0..=257).chain([383, 384, 385, 447, 448, 449]) {
        for shift in 0..=15 {
            let mut bytes = vec![GUARD; shift + len + 8];
            for (index, byte) in bytes[shift..shift + len].iter_mut().enumerate() {
                *byte = index.wrapping_mul(131) as u8;
            }
            let mut want = bytes[shift..shift + len].to_vec();
            want.unmask(mask);

            for target in 0..=15 {
                let mut copied = vec![GUARD; target + len + 8];
                let output = mask::Key::new(mask).copy(
                    &mut copied[target..target + len],
                    &bytes[shift..shift + len],
                );
                assert_eq!(output, want, "len={len}, source={shift}, target={target}");
                assert!(copied[..target].iter().all(|&byte| byte == GUARD));
                assert!(copied[target + len..].iter().all(|&byte| byte == GUARD));
            }

            let output = mask::Key::new(mask).apply(&mut bytes[shift..shift + len]);
            assert_eq!(output, want, "len={len}, shift={shift}");
            assert!(bytes[..shift].iter().all(|&byte| byte == GUARD));
            assert!(bytes[shift + len..].iter().all(|&byte| byte == GUARD));
        }
    }
}

#[test]
fn cursor_keeps_phase() {
    let mask = [0x37, 0xfa, 0x21, 0x3d];
    let input: Vec<u8> = (0usize..259)
        .map(|index| index.wrapping_mul(193) as u8)
        .collect();
    let mut want = input.clone();
    want.unmask(mask);

    for first in 0..=input.len() {
        for second in first..=input.len() {
            let mut actual = input.clone();
            let mut cursor = mask::Key::new(mask).cursor();
            let (a, rest) = actual.split_at_mut(first);
            let (b, c) = rest.split_at_mut(second - first);
            assert_eq!(cursor.apply(a), &want[..first]);
            assert_eq!(cursor.apply(b), &want[first..second]);
            assert_eq!(cursor.apply(c), &want[second..]);
            assert_eq!(actual, want, "splits=({first}, {second})");
            assert!(cursor.key() == mask::Key::new(mask).advanced(input.len()));
        }
    }
}
