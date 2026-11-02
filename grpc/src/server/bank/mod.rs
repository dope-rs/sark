pub(in crate::server) mod lease;
mod node;

use std::{cell, mem, num, ptr};

use lease::Lease;
use node::Node;
use o3::{
    alloc,
    alloc::array,
    depot::{capacity::Count, index},
};
use sark_core::manifold::server::app;

use crate::server::{
    config,
    runtime::{state, state::active::Signals},
};

/// Thread-local readiness residents borrowed by one server plan and its calls.
pub struct Bank {
    pub(in crate::server) limits: config::Limits,
    block: array::Block<Node, num::NonZero<u32>>,
    free: cell::Cell<Option<ptr::NonNull<Node>>>,
}

impl Bank {
    pub fn try_new(cap: num::NonZero<u32>, limits: config::Limits) -> Result<Self, alloc::Error> {
        let domain = index::Domain::from(Count::new(limits.calls.get()));
        let mut first = None;
        let block = array::Block::try_new(cap, ())?.try_init(|slot| {
            let node = Node {
                value: Signals::try_new(domain)?,
                next: cell::Cell::new(first),
            };
            first = Some(slot);
            Ok::<_, alloc::Error>(node)
        })?;
        Ok(Self {
            limits,
            block,
            free: cell::Cell::new(first),
        })
    }

    pub(in crate::server) fn try_take(
        &self,
        cap: app::Capacity,
    ) -> Result<Lease<'_>, state::Error> {
        let first = self.free.get().ok_or(state::Error::Connections {
            capacity: *self.block.extent(),
        })?;
        // SAFETY: the free chain contains only residents of this live allocation.
        // Signals remain initialized and immovable after their reservation ends.
        let node = unsafe { first.as_ref() };
        self.free.set(node.next.get());
        let lease = Lease { owner: self, node };
        let capacity = lease.get().capacity();
        if capacity != cap.streams().get() {
            return Err(state::Error::Extent {
                requested: cap.streams(),
                capacity,
            });
        }
        Ok(lease)
    }
}

const _: () = {
    assert!(
        mem::size_of::<Bank>() == mem::size_of::<config::Limits>() + 3 * mem::size_of::<usize>()
    );
};
