mod seal;

pub(super) use seal::Seal;

/// Closed set of immutable byte owners accepted by a validated request.
pub trait Source: AsRef<[u8]> + Seal {}

impl<T: AsRef<[u8]> + Seal> Source for T {}
