use std::num::NonZero;

use o3::buffer::storage;

use crate::{
    Error,
    segment::{Cursor, input::Input},
    source::string::{Admitted, String},
};

/// A completed value's admitted range and exact output allocation. The enclosing
/// scanner or array retains the same immutable input until replay completes.
pub(in crate::segment) struct Replay {
    cursor: Cursor,
    end: usize,
    output: Vec<u8>,
    string: Option<String<Admitted>>,
}

impl Replay {
    pub(in crate::segment) fn new(
        cursor: Cursor,
        end: usize,
        output: Vec<u8>,
        decoded: bool,
    ) -> Self {
        Self {
            cursor,
            end,
            output,
            string: decoded.then(String::admitted),
        }
    }

    pub(in crate::segment) fn is_done(&self) -> bool {
        self.cursor.absolute == self.end
    }

    pub(in crate::segment) fn is_decoded(&self) -> bool {
        self.string.is_some()
    }

    /// Copies a prefix within both the admitted range and this input-byte budget.
    /// The prefix contains its first byte or a nonempty admitted ASCII run.
    pub(in crate::segment) fn try_copy(
        &mut self,
        input: &Input<'_>,
        budget: NonZero<usize>,
    ) -> crate::Result<NonZero<usize>> {
        let chunk = self.cursor.chunk(input).ok_or(Error::Invalid)?;
        let bytes = chunk.bytes();
        let limit = bytes
            .len()
            .min(self.end - self.cursor.absolute)
            .min(budget.get());
        let bytes = &bytes[..limit];
        let first = *bytes.first().ok_or(Error::Invalid)?;
        let copied = if let Some(string) = &mut self.string {
            let skipped = string.skip(bytes);
            if skipped.is_empty() {
                self.output
                    .extend_from_slice(string.decode(first).as_slice());
                &bytes[..1]
            } else {
                self.output.extend_from_slice(skipped);
                skipped
            }
        } else {
            self.output.extend_from_slice(bytes);
            bytes
        };
        self.cursor.advance(chunk.advance(copied));
        Ok(NonZero::<usize>::MIN.saturating_add(copied.len() - 1))
    }

    pub(in crate::segment) fn try_finish(&mut self) -> crate::Result<storage::Shared> {
        storage::Shared::try_from(&mut self.output).map_err(Error::Exhausted)
    }
}
