use std::num::NonZero;

use crate::{
    h1::{field, head::Boundary},
    request::span,
};

#[derive(Clone, Copy)]
pub(super) struct Tail {
    fields: span::Span,
    wire: NonZero<u32>,
}

impl Tail {
    pub(super) fn new(start: u32, end: field::End) -> Result<Self, Boundary> {
        let fields = span::Span::from_range(start as usize..end.fields() as usize)
            .map_err(Boundary::Span)?;
        Ok(Self {
            fields,
            wire: end.wire_len(),
        })
    }

    pub(super) const fn fields(self) -> span::Span {
        self.fields
    }

    pub(super) const fn wire_len(self) -> NonZero<u32> {
        self.wire
    }
}

const _: () = {
    assert!(core::mem::size_of::<Tail>() == 12);
    assert!(core::mem::size_of::<Option<Tail>>() == 12);
};
