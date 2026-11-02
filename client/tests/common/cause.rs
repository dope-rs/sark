use std::cell::Cell;

pub struct Detail<'a> {
    pub message: &'a str,
    pub drops: &'a Cell<usize>,
}

impl Drop for Detail<'_> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}
