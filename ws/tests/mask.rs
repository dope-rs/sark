use sark_ws::mask::Mask;

fn naive_unmask(buf: &mut [u8], mask: [u8; 4]) {
    for (i, b) in buf.iter_mut().enumerate() {
        *b ^= mask[i & 3];
    }
}

#[test]
fn matches_naive_aligned() {
    for &len in &[
        0usize, 1, 3, 4, 7, 15, 16, 17, 31, 32, 33, 64, 65, 1024, 4096,
    ] {
        let payload: Vec<u8> = (0u8..=255).cycle().take(len).collect();
        let mask = [0xCA, 0xFE, 0xBA, 0xBE];

        let mut a = payload.clone();
        let mut b = payload.clone();
        Mask::unmask_inline(&mut a, mask);
        naive_unmask(&mut b, mask);
        assert_eq!(a, b, "mismatch at len={len}");
    }
}

#[test]
fn round_trip() {
    let mask = [0x12, 0x34, 0x56, 0x78];
    let original: Vec<u8> = (0u8..=255).cycle().take(2003).collect();
    let mut buf = original.clone();
    Mask::unmask_inline(&mut buf, mask);
    Mask::unmask_inline(&mut buf, mask);
    assert_eq!(buf, original);
}
