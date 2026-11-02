use dope_quic::wire::varint::VarInt;
use sark_h3::frame::{self, Payload};

pub mod capacity;
pub mod promise;
pub mod qpack;
pub mod validate;

pub trait Frame: AsRef<[u8]> + Sized {
    fn try_frame(self, kind: VarInt) -> Result<Vec<u8>, frame::EncodeError> {
        let mut wire = Vec::new();
        Payload::encode(kind, self.as_ref(), &mut wire)?;
        Ok(wire)
    }
}
impl<T: AsRef<[u8]>> Frame for T {}
