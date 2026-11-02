/// Replay safety of a failed logical HTTP call.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Safety {
    /// No request from the logical call entered egress.
    BeforeSubmit,
    /// At least one request entered egress before the call failed.
    PossiblyCommitted,
}

impl Safety {
    pub(crate) const fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::PossiblyCommitted, _) | (_, Self::PossiblyCommitted) => Self::PossiblyCommitted,
            (Self::BeforeSubmit, Self::BeforeSubmit) => Self::BeforeSubmit,
        }
    }
}
