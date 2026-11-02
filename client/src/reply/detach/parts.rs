use crate::reply::detach::Part;

/// Exact set of response components sharing one failed allocation.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
#[derive(Debug)]
pub struct Parts(u8);

impl Parts {
    pub const fn contains(self, part: Part) -> bool {
        self.0 & bit(part) != 0
    }

    pub(in crate::reply) const fn empty() -> Self {
        Self(0)
    }

    pub(in crate::reply) const fn with(self, part: Part) -> Self {
        Self(self.0 | bit(part))
    }
}

const fn bit(part: Part) -> u8 {
    1 << part as u8
}

const _: () = assert!(size_of::<Parts>() == 1);
