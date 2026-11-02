use crate::client::state::{calls, output};

/// The single observed tail state under exclusive pending-store admission.
pub(in crate::client) enum Tail<'a> {
    Empty(calls::Space<'a>),
    Pending(&'a mut output::Pending),
}
