use crate::link::Flush;

#[must_use]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::manifold) struct Report {
    flush: Flush,
    released: bool,
}

impl Report {
    pub(super) const fn new(flush: Flush, released: bool) -> Self {
        Self { flush, released }
    }

    pub(in crate::manifold) const fn flush(self) -> Flush {
        self.flush
    }

    pub(in crate::manifold) const fn has_release(self) -> bool {
        self.released
    }

    pub(super) const fn merged(mut self, next: Self) -> Self {
        self.released |= next.released;
        self.flush = next.flush;
        self
    }
}

const _: () = assert!(core::mem::size_of::<Report>() == 2);
