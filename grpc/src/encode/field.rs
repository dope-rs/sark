use o3::buffer;

use crate::{Status, encode};

/// A borrowed protobuf bytes field.
pub struct Field<'a, const TAG: u32> {
    value: &'a [u8],
}

impl<'a, const TAG: u32> Field<'a, TAG> {
    pub fn new(value: &'a [u8]) -> Self {
        const { assert!(TAG > 0 && TAG < 1 << 29 && !(TAG >= 19000 && TAG < 20000)) };
        Self { value }
    }
}

impl<const TAG: u32> encode::Plan for Field<'_, TAG> {
    fn write<S: encode::Sink>(self, sink: S) -> Result<S::Output, Status> {
        if self.value.is_empty() {
            return sink.try_write(None::<buffer::raw::Slices<'_, 0>>);
        }
        let key: &[u8] = const {
            let value = ((TAG as u64) << 3) | 2;
            match prost::encoding::encoded_len_varint(value) {
                1 => &bytes::<1>(value),
                2 => &bytes::<2>(value),
                3 => &bytes::<3>(value),
                4 => &bytes::<4>(value),
                _ => &bytes::<5>(value),
            }
        };
        let value = self.value.len() as u64;
        let length: &[u8] = match value {
            ..=0x7f => &bytes::<1>(value),
            ..=0x3fff => &bytes::<2>(value),
            ..=0x1f_ffff => &bytes::<3>(value),
            ..=0xfff_ffff => &bytes::<4>(value),
            ..=0x7_ffff_ffff => &bytes::<5>(value),
            ..=0x3ff_ffff_ffff => &bytes::<6>(value),
            ..=0x1_ffff_ffff_ffff => &bytes::<7>(value),
            ..=0xff_ffff_ffff_ffff => &bytes::<8>(value),
            // A byte slice is at most isize::MAX bytes.
            _ => &bytes::<9>(value),
        };
        sink.try_slices([key, length, self.value])
    }
}

const fn bytes<const N: usize>(mut value: u64) -> [u8; N] {
    let mut bytes = [0; N];
    let mut index = 0;
    while index + 1 < N {
        bytes[index] = value as u8 | 0x80;
        value >>= 7;
        index += 1;
    }
    bytes[index] = value as u8;
    bytes
}

const _: () = {
    assert!(size_of::<Field<'static, 1>>() == size_of::<&[u8]>());
    assert!(size_of::<Option<Field<'static, 1>>>() == size_of::<Field<'static, 1>>());
};
