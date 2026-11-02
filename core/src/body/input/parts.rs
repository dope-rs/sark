use sark_http::payload::{owned, response::Body as _};

use crate::body::{credit, input::private};

/// Segment input whose accounting is carried by its owner or checked on transfer.
pub trait Parts<'d>: private::Private + IntoIterator<Item = owned::Segment<'d>> {
    const EXACT: bool;

    fn dimensions(&self) -> credit::Amount;
}

impl<'d> Parts<'d> for owned::Segments<'d> {
    const EXACT: bool = true;

    fn dimensions(&self) -> credit::Amount {
        credit::Amount::new(self.len(), self.count())
    }
}
