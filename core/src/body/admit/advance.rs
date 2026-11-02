use crate::{body::admit, manifold::stream::Boundary};

pub(in crate::body) struct Advance<'a> {
    pub(super) track: &'a mut admit::Track<Boundary>,
    pub(super) next: usize,
}

impl Advance<'_> {
    pub(in crate::body) fn commit(self) {
        self.track.len = self.next;
    }
}

const _: () = assert!(core::mem::size_of::<Advance<'_>>() == 2 * core::mem::size_of::<usize>());
