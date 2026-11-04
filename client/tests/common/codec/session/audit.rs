use std::cell::{Cell, RefCell};

use dope_test::harness::signals::Progress;
use sark_client::protocol::{
    codec::output::{Completion, Output},
    response::Event,
};

use crate::common::codec::session::Failure;

#[derive(Debug, Default)]
pub struct Audit {
    pub attempts: [Cell<u32>; 3],
    pub accepted: [Cell<u32>; 3],
    pub queries: Cell<u32>,
    pub blocked: Cell<u32>,
    pub connected: Cell<u32>,
    pub retired: Cell<u32>,
    failure: RefCell<Option<Failure>>,
}

impl Audit {
    pub fn into_result(mut self) -> Result<Self, Failure> {
        match self.failure.get_mut().take() {
            Some(error) => Err(error),
            None => Ok(self),
        }
    }

    fn has_failed(&self) -> bool {
        self.failure.borrow().is_some()
    }

    pub(super) fn fail(&self, error: Failure) {
        self.failure.borrow_mut().get_or_insert(error);
    }

    pub(super) fn record(&self, index: Option<usize>, output: Output<'_>) {
        let completion = output.completion();
        let mut count = 0;
        for event in output.into_events() {
            count += 1;
            match (index, event) {
                (Some(0), Ok(Event::Head(head)))
                    if head.status().as_u16() == 200 && completion == Completion::Complete =>
                {
                    self.accepted[0].set(self.accepted[0].get() + 1);
                }
                (Some(1), Ok(Event::Head(head)))
                    if head.status().as_u16() == 201 && completion == Completion::Pending =>
                {
                    self.accepted[1].set(self.accepted[1].get() + 1);
                }
                (Some(2), Ok(Event::Data(data)))
                    if data.as_slice() == b"x" && completion == Completion::Complete =>
                {
                    self.accepted[2].set(self.accepted[2].get() + 1);
                }
                (_, Err(error)) => self.fail(Failure::Protocol(error)),
                _ => self.fail(Failure::UnexpectedEvent),
            }
        }
        if count != 1 {
            self.fail(Failure::UnexpectedEvent);
        }
    }
}

impl Progress for Audit {
    fn hits(&self) -> u32 {
        u32::from(self.accepted[2].get() != 0 || self.has_failed())
    }
}
