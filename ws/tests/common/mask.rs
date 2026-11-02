pub trait Mask {
    fn unmask(&mut self, mask: [u8; 4]);
}

impl Mask for [u8] {
    fn unmask(&mut self, mask: [u8; 4]) {
        for (i, b) in self.iter_mut().enumerate() {
            *b ^= mask[i & 3];
        }
    }
}
