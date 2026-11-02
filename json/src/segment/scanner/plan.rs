use std::num::NonZero;

use o3::alloc::VecExt as _;

use crate::{
    Error,
    segment::{Cursor, Document, Projection, Stored, ValueKind, cursor::Replay},
};

/// Decode plans originate from validated string fields of the same document.
/// The document retains their original ranges until the copy commits.
pub(super) enum Plan {
    Fields { index: usize },
    Copy { field: usize, replay: Replay },
}

pub(super) enum Prepare {
    Complete,
    Pending(NonZero<usize>),
}

impl Plan {
    fn copy(field: usize, cursor: Cursor, end: usize, output: Vec<u8>, decoded: bool) -> Self {
        Self::Copy {
            field,
            replay: Replay::new(cursor, end, output, decoded),
        }
    }

    pub(super) fn try_step(
        &mut self,
        scan: &mut Document<'_>,
        budget: NonZero<usize>,
    ) -> crate::Result<Prepare> {
        match self {
            Self::Copy { field, replay } => {
                if !replay.is_done() {
                    let copied = replay.try_copy(&scan.input, budget)?;
                    return Ok(Prepare::Pending(copied));
                }
                let value = scan
                    .fields
                    .get_mut(*field)
                    .and_then(Option::as_mut)
                    .ok_or(Error::Invalid)?;
                let output = replay.try_finish()?;
                if replay.is_decoded() {
                    value.decoded = Some(output);
                } else {
                    value.stored = Stored::Copied(output);
                }
                *self = Self::Fields { index: *field + 1 };
                Ok(Prepare::Pending(NonZero::<usize>::MIN))
            }
            Self::Fields { index } => {
                let fields = scan.fields.len();
                if *index != fields {
                    let field = *index;
                    let Some(value) = scan.fields.get(field).and_then(Option::as_ref) else {
                        *index += 1;
                        return Ok(Prepare::Pending(NonZero::<usize>::MIN));
                    };
                    if value.projection == Projection::Frame
                        && let ValueKind::String { escapes } = value.kind
                        && escapes != 0
                    {
                        let Stored::Original(range) = &value.stored else {
                            return Err(Error::Invalid);
                        };
                        // The captured string includes both quotes and its admitted escape count.
                        let start = range.start + 1;
                        let end = range.end - 1;
                        let out_len = end - start - escapes;
                        let cursor = Cursor::at(&scan.input, start).ok_or(Error::Invalid)?;
                        let output = Vec::try_new(out_len).map_err(Error::Allocation)?;
                        *self = Self::copy(field, cursor, end, output, true);
                        return Ok(Prepare::Pending(NonZero::<usize>::MIN));
                    }
                    if matches!(value.projection, Projection::Bytes | Projection::Frame)
                        && let Stored::Original(range) = &value.stored
                        && scan.input.needs_copy(range)?
                    {
                        let len = range.len();
                        let cursor = Cursor::at(&scan.input, range.start).ok_or(Error::Invalid)?;
                        let output = Vec::try_new(len).map_err(Error::Allocation)?;
                        *self = Self::copy(field, cursor, range.end, output, false);
                        return Ok(Prepare::Pending(NonZero::<usize>::MIN));
                    }
                    *index += 1;
                    return Ok(Prepare::Pending(NonZero::<usize>::MIN));
                }
                Ok(Prepare::Complete)
            }
        }
    }
}
