use o3::buffer::{Owned, Shared};
use sark_core::http::EncodedBody;

use crate::JsonEncode;
use crate::encode::SliceWriter;

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
        let mut out = Owned::with_capacity(encoded_len);
        self.0.write_into(&mut out);
        assert_eq!(out.len(), encoded_len, "JsonEncode length mismatch");
        out.freeze()
    }
}
