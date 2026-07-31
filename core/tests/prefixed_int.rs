use sark_core::http::{PrefixedInt, PrefixedIntError};

#[test]
fn prefixed_integer_round_trips_all_widths_and_boundaries() {
    const VALUES: &[u64] = &[
        0,
        1,
        30,
        31,
        32,
        126,
        127,
        128,
        255,
        256,
        u32::MAX as u64,
        u64::MAX,
    ];

    macro_rules! check_width {
        ($bits:literal, $prefix:literal) => {
            for &value in VALUES {
                let integer = PrefixedInt::<$bits>::new(value);
                let mut encoded = Vec::new();
                integer.encode($prefix, &mut encoded);
                assert_eq!(integer.encoded_len(), encoded.len());
                assert_eq!(
                    PrefixedInt::<$bits>::decode(&encoded),
                    Ok((integer, encoded.len()))
                );
            }
        };
    }

    check_width!(1, 0x80);
    check_width!(2, 0x80);
    check_width!(3, 0x80);
    check_width!(4, 0x80);
    check_width!(5, 0x80);
    check_width!(6, 0x80);
    check_width!(7, 0x80);
    check_width!(8, 0x00);
}

#[test]
fn prefixed_integer_distinguishes_partial_input_from_overflow() {
    assert_eq!(
        PrefixedInt::<5>::decode(&[0x1f, 0x80]),
        Err(PrefixedIntError::NeedMore)
    );
    assert_eq!(
        PrefixedInt::<5>::decode(&[
            0x1f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01
        ]),
        Err(PrefixedIntError::Overflow)
    );
}

#[test]
fn prefixed_integer_is_zero_cost_nominally() {
    assert_eq!(
        core::mem::size_of::<PrefixedInt<5>>(),
        core::mem::size_of::<u64>()
    );
    assert_eq!(
        core::mem::align_of::<PrefixedInt<5>>(),
        core::mem::align_of::<u64>()
    );
}
