use o3::buffer::storage;

use crate::owner::evidence;

pub(crate) fn into(out: &mut [u8], bytes: &[u8]) -> Option<usize> {
    let out = out.get_mut(..bytes.len())?;
    out.copy_from_slice(bytes);
    Some(bytes.len())
}

/// One exact copied owner and the evidence for that copy.
pub struct Body {
    pub(in crate::owner::handoff) body: storage::Shared,
}

impl Body {
    pub(in crate::owner::handoff) const fn new(body: storage::Shared) -> Self {
        Self { body }
    }

    pub(in crate::owner::handoff) fn copy(&self) -> evidence::Copy {
        evidence::Copy::borrowed(self.body.len())
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
