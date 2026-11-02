use o3::buffer::{resident, segments, segments::assembly};

use crate::payload::owned::Segments;

pub(super) enum State<'d> {
    Input(Segments<'d>),
    Copying(segments::Flatten<resident::Shared<'d>>),
}

/// Rejected input preserves both the body and the unaccepted segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Finished,
    Assembly(assembly::Error),
}
