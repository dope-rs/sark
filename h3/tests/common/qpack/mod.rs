use o3::{alloc, buffer::write::Sink};
use sark_h3::qpack::encoder;
use sark_http::field::{literal, metadata::integer};

use crate::common::{conn::build::Fields as _, field::entry};

pub enum Instruction<'a> {
    Capacity(u64),
    Insert { name: &'a [u8], value: &'a [u8] },
    Duplicate(u64),
}

impl Instruction<'_> {
    pub fn try_encode(&self, out: &mut Vec<u8>) -> Result<(), alloc::Error> {
        match self {
            Self::Capacity(size) => out.write_slice(
                integer::PrefixedInt::<5>::new(*size)
                    .pack::<11>(0x20)
                    .as_ref(),
            ),
            Self::Duplicate(relative) => out.write_slice(
                integer::PrefixedInt::<5>::new(*relative)
                    .pack::<11>(0)
                    .as_ref(),
            ),
            Self::Insert { name, value } => out.write_slices([
                integer::PrefixedInt::<5>::new(name.len() as u64)
                    .pack::<11>(0x40)
                    .as_ref(),
                name,
                integer::PrefixedInt::<7>::new(value.len() as u64)
                    .pack::<11>(0)
                    .as_ref(),
                value,
            ]),
        }
    }
}

use sark_http::field;

mod wire;
pub use wire::Wire;

pub struct NeverValue<'a>(pub field::Lower<&'a [u8]>, pub field::Value<&'a [u8]>);

impl field::Source for NeverValue<'_> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field>) {
        sink.encoded_with(self.0, self.1, field::Indexing::Never);
    }
}

const NAME: &[u8] = b"x-discard";
const VALUE: &[u8] = b"bad\0value";
fn request() -> Result<Vec<u8>, encoder::Error> {
    [
        literal!(entry(b":method", b"GET")),
        literal!(entry(b":scheme", b"https")),
        literal!(entry(b":authority", b"example.test")),
        literal!(entry(b":path", b"/")),
    ]
    .try_block()
}
impl Instruction<'_> {
    pub fn bad_indexed() -> Result<(Vec<u8>, Vec<u8>), encoder::Error> {
        let mut wire = Vec::new();
        wire.write_byte(2)?;
        Instruction::Capacity(256).try_encode(&mut wire)?;
        Instruction::Insert {
            name: NAME,
            value: VALUE,
        }
        .try_encode(&mut wire)?;
        let mut block = request()?;
        block[0] = 2;
        block.write_byte(0x80)?;
        Ok((wire, block))
    }
}
