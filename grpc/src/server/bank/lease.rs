use std::{mem, ptr};

use crate::server::{Bank, bank::node::Node, runtime::state::active::Signals};

/// One connection's reservation; readiness references retain the bank's loan.
#[must_use = "dropping the lease returns its reservation"]
pub(in crate::server) struct Lease<'a> {
    pub(super) owner: &'a Bank,
    pub(super) node: &'a Node,
}

impl<'a> Lease<'a> {
    pub(in crate::server) const fn get(&self) -> &'a Signals {
        &self.node.value
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        self.node.next.set(self.owner.free.get());
        self.owner.free.set(Some(ptr::NonNull::from(self.node)));
    }
}

const _: () = {
    assert!(mem::size_of::<Lease<'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Lease<'static>>>() == mem::size_of::<Lease<'static>>());
};
