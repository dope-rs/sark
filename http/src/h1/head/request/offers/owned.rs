use o3::buffer::storage;

use crate::h1::{head::request::offers::View, switch};

pub struct Owned {
    pub(super) bytes: storage::Exact,
}

impl Owned {
    pub fn select(&self, selected: &[u8]) -> Result<switch::Selection, switch::UpgradeError> {
        switch::Selection::from_offers(View::from_owned(self), selected)
    }
}

const _: () = assert!(size_of::<Owned>() == size_of::<storage::Exact>());
