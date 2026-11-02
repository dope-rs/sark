mod encoder;
pub use encoder::Encoder;
pub mod owner;

use sark_h3::qpack;

pub trait Section {
    fn is_dynamic(&self) -> bool;
    fn into_wire(self) -> Vec<u8>;
}

impl Section for qpack::Section {
    fn is_dynamic(&self) -> bool {
        let mut prefix = Vec::new();
        self.write_prefix(&mut prefix);
        matches!(prefix.first(), Some(byte) if *byte != 0)
    }

    fn into_wire(self) -> Vec<u8> {
        let mut wire = Vec::new();
        self.write_prefix(&mut wire);
        wire.extend_from_slice(self.lines());
        wire
    }
}
