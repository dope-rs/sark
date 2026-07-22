use o3::buffer::Shared;

use crate::Result;
use crate::encode::{Write, Writer};

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

    fn write_into<W: Write>(&self, w: &mut W);

    fn write_json(&self, out: &mut Vec<u8>) {
        let expected = self.json_len();
        let mut w = Writer::new(out, expected);
        self.write_into(&mut w);
        assert_eq!(w.finish(), expected, "JsonEncode length mismatch");
    }

    fn encode_json(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.json_len());
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

    fn write_into<W: Write>(&self, w: &mut W) {
        (*self).write_into(w)
    }

    fn write_json(&self, out: &mut Vec<u8>) {
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
