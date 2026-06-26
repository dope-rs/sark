use o3::buffer::{Owned, Shared};

use crate::Result;
use crate::encode::Writer;

pub trait JsonDecode: Sized {
    fn decode_json(input: Shared) -> Result<Self>;

    fn decode_json_borrowed(input: &[u8]) -> Result<Self> {
        Self::decode_json(Shared::copy_from_slice(input))
    }
}

pub trait JsonScan: Sized {
    fn scan_json<'a, I>(chunks: I) -> Result<Self>
    where
        I: IntoIterator<Item = &'a [u8]>;
}

pub trait JsonEncode: Sized {
    fn json_len(&self) -> usize;

    fn write_into(&self, w: &mut Writer<'_>);

    fn write_json(&self, out: &mut Owned) {
        let mut w = Writer::new(out, self.json_len());
        self.write_into(&mut w);
        w.finish();
    }

    fn encode_json(&self) -> Owned {
        let mut out = Owned::new();
        self.write_json(&mut out);
        out
    }
}

pub trait JsonPreserve {
    fn raw_json(&self) -> Option<&Shared>;
}

impl<T> JsonEncode for &T
where
    T: JsonEncode,
{
    fn json_len(&self) -> usize {
        (*self).json_len()
    }

    fn write_into(&self, w: &mut Writer<'_>) {
        (*self).write_into(w)
    }

    fn write_json(&self, out: &mut Owned) {
        (*self).write_json(out)
    }
}

impl<T> JsonPreserve for &T
where
    T: JsonPreserve + ?Sized,
{
    fn raw_json(&self) -> Option<&Shared> {
        (*self).raw_json()
    }
}
