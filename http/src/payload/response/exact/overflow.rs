use std::num::NonZero;

/// Encoded-length arithmetic exceeded the representable domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Overflow {
    pub current: usize,
    pub added: NonZero<usize>,
}

impl Overflow {
    pub fn try_add(current: usize, added: usize) -> Result<usize, Self> {
        current.checked_add(added).ok_or_else(|| Self {
            current,
            added: NonZero::<usize>::MIN.saturating_add(added - 1),
        })
    }
}
