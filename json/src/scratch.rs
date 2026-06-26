use std::cell::RefCell;

pub struct Scratch<T> {
    slot: RefCell<Vec<T>>,
}

impl<T> Scratch<T> {
    pub const fn new() -> Self {
        Self {
            slot: RefCell::new(Vec::new()),
        }
    }

    pub fn take(&self) -> Vec<T> {
        let mut slot = self.slot.borrow_mut();
        let mut out = std::mem::take(&mut *slot);
        out.clear();
        out
    }

    pub fn give(&self, mut buf: Vec<T>) {
        buf.clear();
        let mut slot = self.slot.borrow_mut();
        if buf.capacity() > slot.capacity() {
            *slot = buf;
        }
    }
}

impl<T> Default for Scratch<T> {
    fn default() -> Self {
        Self::new()
    }
}
