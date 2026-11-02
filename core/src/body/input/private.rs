use sark_http::payload::owned;

pub trait Private {}

impl Private for owned::Segments<'_> {}
