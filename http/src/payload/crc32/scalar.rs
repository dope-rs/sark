//! No CPU on this target admits an accelerated backend.

use crate::payload::crc32::Backend;

/// Uninhabited: no admitted backend exists, so folding uses the table default.
#[derive(Clone, Debug)]
pub(super) enum Scalar {}

impl Backend for Scalar {
    fn admit() -> Option<Self> {
        None
    }
}
