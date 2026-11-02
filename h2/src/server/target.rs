use std::{marker, mem};

use dope::{
    core::driver::{route, schedule::ready::task::owner},
    manifold::network::listener::conn,
};
use o3::depot::queue::indexed::slab;
use sark_core::manifold::{self, task};

use crate::server::task::registry::Tag;

/// Exact H2 task capability retained beside one generated task.
#[doc(hidden)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Target<'d, ID: route::Identity> {
    key: slab::Handle<Tag>,
    brand: marker::PhantomData<(&'d mut &'d conn::Id<'d, ID>, *mut conn::Id<'d, ID>)>,
}

impl<ID: route::Identity> task::Target for Target<'_, ID> {}

impl<'d, ID: route::Identity> owner::Source for Target<'d, ID> {
    type Identity = conn::Id<'d, ID>;
}

impl<'d, ID: route::Identity> Target<'d, ID> {
    pub(in crate::server) fn new(key: slab::Handle<Tag>) -> Self {
        Self {
            key,
            brand: marker::PhantomData,
        }
    }

    pub(in crate::server) fn key(self) -> slab::Handle<Tag> {
        self.key
    }
}

const _: () = {
    assert!(mem::size_of::<Target<'static, route::KeyTag<0>>>() == mem::size_of::<u64>());
    assert!(mem::align_of::<Target<'static, route::KeyTag<0>>>() == mem::align_of::<u64>());
};

// SAFETY: `Target` is minted only from this registry's branded slab handle.
unsafe impl<ID: route::Identity> manifold::raw::Target for Target<'_, ID> {}
