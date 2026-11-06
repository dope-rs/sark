use crate::owner::evidence::Cause;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Copy {
    pub read: usize,
    pub written: usize,
    pub cause: Cause,
}

impl Copy {
    pub const fn borrowed(bytes: usize) -> Self {
        Self {
            read: bytes,
            written: bytes,
            cause: Cause::Borrowed,
        }
    }
}
