use sark_h3::qpack::encoder;
use sark_http::field::metadata::{
    huffman::source::Source, integer::PrefixedInt, strings::Encoding,
};

use crate::common::qpack::{NAME, VALUE, request};

pub trait Wire {
    fn bad_literal(self) -> Result<Vec<u8>, encoder::Error>;
}
impl Wire for Encoding {
    fn bad_literal(self) -> Result<Vec<u8>, encoder::Error> {
        let mut block = request()?;
        PrefixedInt::<3>::new(NAME.len() as u64).encode(0x20, &mut block);
        block.extend_from_slice(NAME);
        match self {
            Encoding::Plain => {
                PrefixedInt::<7>::new(VALUE.len() as u64).encode(0, &mut block);
                block.extend_from_slice(VALUE);
            }
            Encoding::Huffman => {
                let value = Source::new(VALUE).try_measure()?;
                PrefixedInt::<7>::new(value.encoded_len() as u64).encode(0x80, &mut block);
                value.encode(&mut block);
            }
        }
        Ok(block)
    }
}
