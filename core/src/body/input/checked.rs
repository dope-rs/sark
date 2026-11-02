use sark_http::payload::owned;

use crate::body::{
    credit,
    input::{Parts, private},
};

/// Arbitrary segment input whose dimensions must be checked during transfer.
pub struct Checked<I>(I);

impl<I> Checked<I> {
    pub const fn new(input: I) -> Self {
        Self(input)
    }
}

impl<I> private::Private for Checked<I> {}

impl<I: IntoIterator> IntoIterator for Checked<I> {
    type Item = I::Item;
    type IntoIter = I::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'d, I: IntoIterator<Item = owned::Segment<'d>>> Parts<'d> for Checked<I> {
    const EXACT: bool = false;

    fn dimensions(&self) -> credit::Amount {
        credit::Amount::default()
    }
}
