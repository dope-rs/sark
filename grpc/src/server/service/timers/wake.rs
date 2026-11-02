use std::cmp;

use dope::core::driver::schedule::clock;

use crate::server::runtime::state::active;

pub(in crate::server) struct Wake<'r, 'd> {
    pub(in crate::server) at: clock::Deadline<'d>,
    pub(super) entry: active::Entry<'r>,
}

impl PartialEq for Wake<'_, '_> {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at
    }
}

impl Eq for Wake<'_, '_> {}

impl PartialOrd for Wake<'_, '_> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Wake<'_, '_> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.at.cmp(&other.at)
    }
}
