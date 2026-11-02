use std::{marker::PhantomData, num};

use dope::core::driver::route::Identity;
use net::pools::entries::Positioned;
use o3::{
    base::cell::region,
    depot::queue::{
        indexed::lanes::{entry, runtime::shared},
        tagged,
    },
};
use tagged::lane;

use crate::client::{dynamic::origin, exchange::net};

type Head<'q, 'd> = shared::Head<'q, 'd, (), num::NonZero<u32>, num::NonZero<u32>>;

pub(in crate::client) struct Tagged<'d, ID: Identity> {
    queue: shared::Fifo<'d, (), num::NonZero<u32>, num::NonZero<u32>>,
    route: PhantomData<fn(ID) -> ID>,
}

impl<'d, ID: Identity> Tagged<'d, ID> {
    pub(in crate::client) fn try_new(
        plan: net::Plan,
        tags: num::NonZero<u32>,
        token: &region::Token<region::Identity<'d>>,
    ) -> Result<Self, tagged::Error<num::NonZero<u32>, num::NonZero<u32>>> {
        let capacity = plan.pool.capacity();
        Ok(Self {
            queue: shared::Fifo::try_from(capacity, tags, token)?,
            route: PhantomData,
        })
    }

    pub(in crate::client) fn lane<'q>(
        &'q self,
        index: usize,
    ) -> Result<Head<'q, 'd>, lane::Error<num::NonZero<u32>>> {
        self.queue.head(index)
    }
}

impl<'r, 'q, 'd, ID: Identity> net::ReadyQueue<'d, ID, origin::Key<'r, 'q, 'd, ID>>
    for &'q Tagged<'d, ID>
{
    /// The bound directory entry owns its generation and fixed origin tag.
    /// This queue stores only membership, removed before the slot is reusable.
    fn push_back<S>(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        bound: &Positioned<'_, '_, 'd, ID, origin::Key<'r, 'q, 'd, ID>, S>,
    ) -> Result<(), entry::Error<num::NonZero<u32>>> {
        let mut lane = bound.tag().lane().borrow(token);
        if let entry::State::Vacant(vacant) = lane.entry(bound.connection().index().into_usize())? {
            vacant.insert::<false>(());
        }
        Ok(())
    }

    fn remove<S>(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        bound: &Positioned<'_, '_, 'd, ID, origin::Key<'r, 'q, 'd, ID>, S>,
    ) {
        bound
            .tag()
            .lane()
            .take(token, bound.connection().index().into_usize(), ());
    }

    fn pop_front(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        tag: origin::Key<'r, 'q, 'd, ID>,
    ) -> Option<usize> {
        tag.lane()
            .borrow(token)
            .pop_entry()
            .map(|(index, ())| index)
    }

    fn front(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        tag: origin::Key<'r, 'q, 'd, ID>,
    ) -> Option<usize> {
        tag.lane()
            .borrow(token)
            .front()
            .map(|entry| entry.key().get())
    }
}
