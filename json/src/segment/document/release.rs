use std::{num::NonZero, ops::ControlFlow, task::Poll};

use crate::segment::Document;

/// Completed input and field projections that can only be released.
#[must_use = "step or release the retained owners"]
pub struct Release<'d> {
    pub(super) document: Document<'d>,
}

impl Release<'_> {
    /// Charges one unit for each field slot or input owner retired.
    /// Preparation visits each field once and either copies or decodes it,
    /// so each slot retains at most one projection owner.
    pub fn step(mut self, budget: NonZero<usize>) -> ControlFlow<(), Self> {
        let mut left = budget.get();
        if self.retire(&mut left).is_ready() {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(self)
        }
    }

    pub fn retire(&mut self, budget: &mut usize) -> Poll<()> {
        self.document.retire(budget)
    }

    pub(in crate::segment) fn drain(
        &mut self,
        budget: &mut usize,
    ) -> (Poll<()>, crate::segment::scanner::Released) {
        self.document.drain(budget)
    }
}
