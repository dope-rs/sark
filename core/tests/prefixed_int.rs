use sark_core::http::{PrefixedInt, PrefixedIntError};

#[test]
fn prefixed_integer_round_trips_all_widths_and_boundaries() {
    const VALUES: &[u64] = &[0, 1, 30, 31, 32, 126, 127, 128, 255, 256, u32::MAX as u64];

    for prefix_bits in 1..=8 {
        for &value in VALUES {
            let mut encoded = Vec::new();
            PrefixedInt::encode(value, prefix_bits, 0xa0, &mut encoded);
            assert_eq!(
                PrefixedInt::decode(&encoded, prefix_bits),
                Ok((value, encoded.len()))
            );
        }
    }
}

#[test]
fn prefixed_integer_distinguishes_partial_input_from_overflow() {
    assert_eq!(
        PrefixedInt::decode(&[0x1f, 0x80], 5),
        Err(PrefixedIntError::NeedMore)
    );
    assert_eq!(
        PrefixedInt::decode(
            &[
                0x1f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01
            ],
            5
        ),
        Err(PrefixedIntError::Overflow)
    );
}
