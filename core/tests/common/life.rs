use std::fmt;

use sark_core::life::{self, audit};

/// Counts lifecycle callbacks without retaining their connection or stream owners.
pub struct Events<F> {
    pub events: usize,
    pub audits: usize,
    pub diagnostic: F,
}

impl<C: Copy, S: Copy, F: FnMut(&dyn fmt::Debug)> life::Observer<C, S> for Events<F> {
    fn observe(&mut self, _: life::Event<C, S>) {
        self.events += 1;
    }
    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E) {
        (self.diagnostic)(cause);
    }

    fn audit(&mut self, _: C, _: audit::Snapshot) {
        self.audits += 1;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct NativeStream(pub u32);
