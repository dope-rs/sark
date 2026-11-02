use crate::owner::evidence::Cause;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Copy {
    pub read: usize,
    pub written: usize,
    pub cause: Cause,
}
