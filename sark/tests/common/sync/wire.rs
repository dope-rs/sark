pub struct Window {
    stream: u32,
    value: u32,
}

impl Window {
    pub const fn new(stream: u32, value: u32) -> Self {
        Self { stream, value }
    }

    pub fn encode(self) -> [u8; 13] {
        let mut frame = [0_u8; 13];
        frame[2] = 4;
        frame[3] = 8;
        frame[5..9].copy_from_slice(&(self.stream & 0x7fff_ffff).to_be_bytes());
        frame[9..].copy_from_slice(&(self.value & 0x7fff_ffff).to_be_bytes());
        frame
    }
}
