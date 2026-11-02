use o3::buffer::{resident, storage};

use crate::{owner::handoff::stage::Body as _, request::body};

/// A contiguous projection retaining its original storage owner.
#[derive(Clone, Copy)]
pub enum View<'req, 'd> {
    Slice(&'req [u8]),
    Resident(&'req resident::Shared<'d>),
    Shared(&'req storage::Shared),
}

impl<'req, 'd> View<'req, 'd> {
    pub fn as_slice(&self) -> &'req [u8] {
        match *self {
            Self::Slice(bytes) => bytes,
            Self::Resident(owner) => owner.as_slice(),
            Self::Shared(owner) => owner.as_ref(),
        }
    }
}

impl<'req, 'd> From<View<'req, 'd>> for body::Ref<'req, 'd> {
    fn from(view: View<'req, 'd>) -> Self {
        let owner = match view {
            View::Slice(bytes) => return Self::from_slice(bytes),
            View::Resident(owner) => body::Retained::Resident(owner),
            View::Shared(owner) => body::Retained::Shared(owner),
        };
        Self {
            storage: body::RefStorage::Retained(owner),
            len: owner.defer(),
        }
    }
}

const _: () = {
    assert!(size_of::<View<'static, 'static>>() == 3 * size_of::<usize>());
    assert!(size_of::<Option<View<'static, 'static>>>() == size_of::<View<'static, 'static>>());
};
