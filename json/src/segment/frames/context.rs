use sark_http::request;

/// Contexts that preserve the exact bytes admitted by the selected grammar.
pub trait Context: Copy {}

impl Context for () {}
impl Context for request::Id<'_> {}
