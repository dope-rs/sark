use crate::field::metadata::{
    block,
    section::{self, storage},
};

/// Unbound completion; only the decoder owning its input may publish a head.
#[doc(hidden)]
pub struct Decoded<P: section::Policy> {
    pub(super) storage: storage::Finished<P::Block>,
    pub(super) pick: P::Selection,
    pub(super) sealed: block::Validation,
    pub(super) outcome: block::Outcome,
    pub(super) framing: block::Framing,
}

impl<P: section::Policy> Decoded<P> {
    pub(in super::super) fn new(
        storage: storage::Finished<P::Block>,
        pick: P::Selection,
        sealed: block::Validation,
        outcome: block::Outcome,
        framing: block::Framing,
    ) -> Self {
        Self {
            storage,
            pick,
            sealed,
            outcome,
            framing,
        }
    }
}
