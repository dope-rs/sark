use dope_core::driver::schedule::sequence;

/// Exact request identity within one driver domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Id<'d>(sequence::Id<'d>);

impl<'d> Id<'d> {
    pub(in crate::request) const fn new(id: sequence::Id<'d>) -> Self {
        Self(id)
    }
}

const _: () = {
    assert!(size_of::<Id<'static>>() == size_of::<usize>());
    assert!(size_of::<Option<Id<'static>>>() == size_of::<usize>());
};
