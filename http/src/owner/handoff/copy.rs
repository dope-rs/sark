use o3::buffer::storage;

use crate::owner::evidence;

/// One exact copied owner and the evidence for that copy.
pub struct Body {
    pub(in crate::owner::handoff) body: storage::Shared,
}

impl Body {
    pub(in crate::owner::handoff) const fn new(body: storage::Shared) -> Self {
        Self { body }
    }

    pub(in crate::owner::handoff) fn copy(&self) -> evidence::Copy {
        let len = self.body.len();
        evidence::Copy {
            read: len,
            written: len,
            cause: evidence::Cause::Borrowed,
        }
    }

    pub(in crate::owner::handoff) fn len(&self) -> usize {
        self.body.len()
    }

    pub fn into_parts(self) -> (storage::Shared, evidence::Copy) {
        let copy = self.copy();
        (self.body, copy)
    }

    pub fn into_body(self) -> storage::Shared {
        self.body
    }
}

const _: () = assert!(size_of::<Body>() == size_of::<storage::Shared>());
