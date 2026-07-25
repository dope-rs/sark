use std::convert::Infallible;

use o3::buffer::{Owned, Shared, SpareWriter};
use sark_core::http::EncodedBody;

use crate::JsonEncode;
use crate::encode::{SliceWriter, Write};

struct ExactWriter<'a, 'buf>(&'a mut SpareWriter<'buf>);

impl Write for ExactWriter<'_, '_> {
    fn put(&mut self, src: &[u8]) {
        self.0
            .try_extend_from_slice(src)
            .expect("JsonEncode wrote beyond json_len");
    }
}

pub struct JsonBody<T>(T);

impl<T> JsonBody<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T> EncodedBody for JsonBody<T>
where
    T: JsonEncode,
{
    fn encoded_len(&self) -> usize {
        self.0.json_len()
    }

    fn encode_into(&self, out: &mut [u8]) {
        let mut writer = SliceWriter::new(out);
        self.0.write_into(&mut writer);
        assert_eq!(writer.finish(), out.len(), "JsonEncode length mismatch");
    }

    fn into_shared(self, encoded_len: usize) -> Shared {
        Owned::try_build_exact(encoded_len, |out| {
            self.0.write_into(&mut ExactWriter(out));
            Ok::<_, Infallible>(())
        })
        .expect("JsonEncode length mismatch")
        .freeze()
    }
}
