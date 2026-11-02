use dope_quic::{
    conn::stream::resident,
    wire::stream::{self, Kind},
};
use o3::depot::arena::{hash::runtime, slab::key};

use crate::{manifold::requests::entries, stream::Target};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::manifold) struct Identity {
    local: resident::Local,
}

impl Identity {
    pub(in crate::manifold) const fn new(local: resident::Local) -> Option<Self> {
        if matches!(local.id().kind(), Kind::ClientBidi) {
            Some(Self { local })
        } else {
            None
        }
    }

    pub(in crate::manifold) const fn stream_id(self) -> stream::Id {
        self.local.id()
    }

    pub(in crate::manifold) const fn local(self) -> resident::Local {
        self.local
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Id {
    identity: Identity,
    entry: key::Handle<runtime::Domain<entries::Tag>>,
}

impl Id {
    pub(super) const fn from_entry(
        id: Identity,
        entry: key::Handle<runtime::Domain<entries::Tag>>,
    ) -> Self {
        Self {
            identity: id,
            entry,
        }
    }

    pub(in crate::manifold) const fn local(self) -> resident::Local {
        self.identity.local()
    }

    pub(in crate::manifold) const fn identity(self) -> Identity {
        self.identity
    }

    pub(in crate::manifold) const fn stream_id(self) -> stream::Id {
        self.identity.stream_id()
    }

    pub(in crate::manifold) const fn target(self) -> Target {
        let local = self.identity.local();
        Target::resident(local.id(), local.key())
    }

    pub(in crate::manifold) const fn slot(self) -> u32 {
        self.entry.index()
    }

    pub(in crate::manifold) const fn entry(self) -> key::Handle<runtime::Domain<entries::Tag>> {
        self.entry
    }
}

const _: () = {
    assert!(core::mem::size_of::<Identity>() == core::mem::size_of::<resident::Local>());
    assert!(
        core::mem::size_of::<Id>()
            == core::mem::size_of::<resident::Local>()
                + core::mem::size_of::<key::Handle<runtime::Domain<entries::Tag>>>()
    );
};
