use encode::varint::Varint;
use o3::buffer;

use crate::{Status, encode};

/// A borrowed protobuf bytes field with its initialized length prefix.
pub struct Field<'a, const TAG: u32> {
    value: &'a [u8],
    length: Varint,
}

impl<'a, const TAG: u32> Field<'a, TAG> {
    pub fn new(value: &'a [u8]) -> Self {
        const { assert!(TAG > 0 && TAG < 1 << 29 && !(TAG >= 19000 && TAG < 20000)) };
        Self {
            value,
            length: Varint::new(value.len() as u64),
        }
    }
}

impl<const TAG: u32> encode::Plan for Field<'_, TAG> {
    fn write<S: encode::Sink>(self, sink: S) -> Result<S::Output, Status> {
        if self.value.is_empty() {
            return sink.try_write(None::<buffer::raw::Slices<'_, 0>>);
        }
        let key = const { Varint::new(((TAG as u64) << 3) | 2) };
        sink.try_slices([key.as_slice(), self.length.as_slice(), self.value])
    }
}

const _: () = {
    assert!(
        size_of::<Field<'static, 1>>()
            == size_of::<&[u8]>() + size_of::<Varint>().next_multiple_of(align_of::<&[u8]>())
    );
    assert!(size_of::<Option<Field<'static, 1>>>() == size_of::<Field<'static, 1>>());
};
