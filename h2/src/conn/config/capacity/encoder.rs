#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Encoder {
    pub table: u32,
    pub header_list: usize,
}

impl Default for Encoder {
    fn default() -> Self {
        use crate::conn::config::{DEFAULT_HPACK_ENCODER_CAPACITY, DEFAULT_MAX_HEADER_LIST_SIZE};

        Self {
            table: DEFAULT_HPACK_ENCODER_CAPACITY,
            header_list: DEFAULT_MAX_HEADER_LIST_SIZE as usize,
        }
    }
}
