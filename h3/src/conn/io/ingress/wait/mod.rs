use dope_quic::wire::stream::Id;
use o3::depot::{
    array::Index,
    queue::{group::capacity, indexed::slab},
};

mod table;

pub(in crate::conn) use table::{Member, Table};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Lane {
    Ready,
    Feedback,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    Slot(slab::Error<(), { u32::MAX }, capacity::Count<2>>),
    Present {
        id: Id,
        queued: Lane,
        requested: Lane,
    },
    Full {
        id: Id,
        lane: Lane,
        capacity: usize,
    },
    Missing {
        expected: Id,
    },
    Mismatch {
        expected: Id,
        actual: Id,
    },
}

impl Lane {
    const fn index(self) -> Index<2> {
        match self {
            Self::Ready => Index::at::<0>(),
            Self::Feedback => Index::at::<1>(),
        }
    }
}
