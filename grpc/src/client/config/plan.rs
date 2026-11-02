use std::num::NonZero;

use o3::{buffer::pool, depot::capacity};

use crate::client::config::{Error, Field, Input, Operation, Output};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Plan {
    pub(super) layout: pool::Layout,
    pub(super) unary: capacity::Count,
    pub(super) slots: usize,
    pub(super) input_max: usize,
    pub(super) request_max: usize,
}

impl Plan {
    pub(super) fn new(
        calls: NonZero<u32>,
        bytes: capacity::Count,
        input: Input,
        output: Output,
    ) -> Result<Self, Error> {
        let calls = calls.get() as usize;
        let unary = super::try_count(Field::UnaryClaims, calls + input.completed())?;
        let slots = super::try_count(Field::CompletionSlots, calls + unary.get())?.get();
        let width = bytes.get().max(1);
        let layout = pool::Layout::new(output.messages(), width).map_err(Error::Pool)?;
        let inbound = input
            .bytes()
            .checked_add(bytes.get())
            .ok_or(Error::Overflow {
                field: Field::InputBytes,
                operation: Operation::Add,
                left: input.bytes(),
                right: bytes.get(),
            })?;
        Ok(Self {
            layout,
            unary,
            slots,
            input_max: inbound,
            request_max: layout.bytes(),
        })
    }
}
