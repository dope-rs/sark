use crate::owner::evidence::Copy;

pub trait Sink {
    const ENABLED: bool;

    fn copied(&mut self, copy: Copy);
}
