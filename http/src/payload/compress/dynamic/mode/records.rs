use crate::payload::compress::dynamic::{hash, table::slot};

#[derive(Clone, Copy)]
#[repr(C)]
pub struct Direct {
    pub(super) slot: slot::Slot,
    digest: hash::Digest,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Staged(pub(super) slot::Slot);

impl Direct {
    pub(crate) const fn new(slot: slot::Slot, digest: hash::Digest) -> Self {
        Self { slot, digest }
    }

    pub(crate) const fn digest(self) -> hash::Digest {
        self.digest
    }
}

impl Staged {
    pub(crate) const fn new(slot: slot::Slot) -> Self {
        Self(slot)
    }

    pub(crate) const fn slot(self) -> slot::Slot {
        self.0
    }
}

const _: () = {
    assert!(core::mem::size_of::<Direct>() == 3 * core::mem::size_of::<u64>());
    assert!(core::mem::size_of::<Staged>() == slot::Slot::BYTES);
};
