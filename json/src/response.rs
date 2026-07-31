use o3::buffer::{CapacityError, ExactBuildError, Owned, Shared, SliceWriter};
use sark_core::http::EncodedBody;

use crate::JsonEncode;

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
    type Error = ExactBuildError<CapacityError>;

    fn encoded_len(&self) -> usize {
        self.0.json_len()
    }

    fn encode_into(&self, out: &mut [u8]) -> Result<(), Self::Error> {
        let expected = out.len();
        let mut writer = SliceWriter::new(out);
        self.0
            .write_into(&mut writer)
            .map_err(ExactBuildError::Build)?;
        let actual = writer.finish();
        if actual != expected {
            return Err(ExactBuildError::LengthMismatch { expected, actual });
        }
        Ok(())
    }

    fn into_shared(self, encoded_len: usize) -> Result<Shared, Self::Error> {
        Owned::try_build_exact(encoded_len, |out| self.0.write_into(out)).map(Owned::freeze)
    }
}
