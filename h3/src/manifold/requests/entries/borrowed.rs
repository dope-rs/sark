use o3::{
    depot,
    depot::{
        arena::{
            hash::{runtime, runtime::entry},
            slab::key,
        },
        index::Initialized,
        queue::{indexed::keyed, links},
    },
};
use sark_core::{body, decode::request, duplex};

use crate::manifold::requests::{entries, entries::Occupied};

pub(super) struct Borrowed<S>(pub(super) S);

// SAFETY: the native occupied guard retains this slot's bounded key and initialized ready link.
unsafe impl<'a, 'd, P: request::Prepared, B: body::Build<'d>, T, D: duplex::Session<'d>>
    depot::raw::KeyBorrow<'a, runtime::Indexed>
    for Borrowed<&'a entry::Live<'_, entries::Slot<'d, P, B, T, D>, entries::Tag, entry::Occupied>>
{
    type Key = key::Handle<runtime::Domain<entries::Tag>>;

    fn parts(self) -> (Self::Key, &'a keyed::Link<Self::Key, runtime::Indexed>) {
        (self.0.key(), &self.0.get().ready)
    }
}

// SAFETY: the live plane retains this owner's original store and exact initialized neighbor.
unsafe impl<'a, 'd, P: request::Prepared, B: body::Build<'d>, T, D: duplex::Session<'d>>
    depot::raw::KeyBorrow<'a, runtime::Indexed>
    for Borrowed<
        depot::raw::Proof<
            (&'a Occupied<'_, 'd, P, B, T, D>, links::Index),
            Initialized<runtime::Indexed>,
        >,
    >
{
    type Key = key::Handle<runtime::Domain<entries::Tag>>;

    fn parts(self) -> (Self::Key, &'a keyed::Link<Self::Key, runtime::Indexed>) {
        // SAFETY: the native guard borrows the unchanged store retained by the live source.
        let live = unsafe { self.0.map(|(owner, index)| (&owner.entry, index)) };
        let (key, slot) = entry::Live::project(live);
        (key, &slot.ready)
    }
}
