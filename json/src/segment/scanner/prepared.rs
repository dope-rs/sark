use std::{num::NonZero, task::Poll};

use crate::segment::{
    Document,
    scanner::{
        Scanning, Stream,
        plan::{Plan, Prepare},
    },
    state::Pin,
};

pub(in crate::segment) struct Prepared<'d> {
    pub(in crate::segment) document: Document<'d>,
    plan: Plan,
    pub(in crate::segment) stream: Stream,
    sweep: bool,
}
impl<'d> Prepared<'d> {
    pub(in crate::segment) fn new(scan: Scanning<'d>) -> Self {
        Self {
            document: Document::new(scan.input, scan.selection.values),
            plan: Plan::Fields { index: 0 },
            stream: scan.stream,
            sweep: false,
        }
    }
    pub(in crate::segment) fn release(self) -> crate::segment::Release<'d> {
        self.document.release()
    }

    pub(in crate::segment) fn step(&mut self, budget: usize) -> crate::Result<Poll<usize>> {
        let mut left = budget;
        while let Some(budget) = NonZero::new(left) {
            if self.sweep {
                let end = self.document.input.count();
                if self
                    .stream
                    .reap(
                        &mut self.document.input,
                        &self.document.fields,
                        end,
                        Pin::Free,
                        &mut left,
                    )
                    .is_ready()
                {
                    return Ok(Poll::Ready(left));
                }
            } else {
                match self.plan.try_step(&mut self.document, budget)? {
                    Prepare::Complete => {
                        left -= 1;
                        self.stream.restart();
                        self.sweep = true;
                    }
                    Prepare::Pending(spent) => left -= spent.get(),
                }
            }
        }
        Ok(Poll::Pending)
    }
}
