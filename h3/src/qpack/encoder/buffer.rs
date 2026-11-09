use std::mem;

pub(super) struct Cache<'a>(pub(super) &'a mut Vec<u8>);

impl Cache<'_> {
    pub(super) fn take(&mut self) -> Vec<u8> {
        mem::take(self.0)
    }

    pub(super) fn recycle(&mut self, mut lines: Vec<u8>) {
        lines.clear();
        if lines.capacity() > self.0.capacity() {
            *self.0 = lines;
        }
    }
}
