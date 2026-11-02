use core::ops;

use sark_http::field::metadata::section::rules;

pub(crate) struct Fragment {
    pub(crate) start: usize,
    pub(crate) len: usize,
    pub(crate) total: usize,
    pub(crate) kind: rules::Kind,
}

impl Fragment {
    pub(super) const fn range(&self) -> ops::Range<usize> {
        self.start..self.start + self.len
    }
}
