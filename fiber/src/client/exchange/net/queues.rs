use std::{marker::PhantomData, num::NonZero};

use dope::core::driver::{
    route::Identity,
    schedule::ready::wake::{
        self,
        queue::{Error, Queue, Registration},
    },
};
use o3::{alloc, base::cell::region, depot::queue};
use queue::{
    indexed::lanes::{self, entry},
    resident::Fixed,
    tagged,
};

use crate::client::exchange::net::pools::entries::Positioned;

pub(in crate::client) trait PoolTag<'d, ID: Identity>: Copy {
    type Ready: ReadyQueue<'d, ID, Self>;
    type Lanes;
    type Connections: AsRef<[Queue<'d, Fixed<1>>]>;

    fn wake(self, queue: &Queue<'d, Self::Lanes>);

    fn register<'target>(
        self,
        queue: &'target Queue<'d, Self::Lanes>,
        entry: &mut Registration<'target, 'd, Self::Lanes>,
        wake: &wake::Waker<'d>,
    ) -> Result<(), Error>;
}

pub(in crate::client) trait ReadyQueue<'d, ID: Identity, Tag: Copy> {
    fn push_back<S>(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        bound: &Positioned<'_, '_, 'd, ID, Tag, S>,
    ) -> Result<(), entry::Error<NonZero<u32>>>;
    fn remove<S>(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        bound: &Positioned<'_, '_, 'd, ID, Tag, S>,
    );
    fn pop_front(&self, token: &mut region::Token<region::Identity<'d>>, tag: Tag)
    -> Option<usize>;
    fn front(&self, token: &mut region::Token<region::Identity<'d>>, tag: Tag) -> Option<usize>;
}

pub(in crate::client) struct FixedReady<'d, ID: Identity> {
    queue: region::Value<region::Identity<'d>, lanes::Fifo<(), 1, NonZero<u32>>>,
    route: PhantomData<fn(ID) -> ID>,
}

impl<'d, ID: Identity> FixedReady<'d, ID> {
    pub(in crate::client) fn try_new(
        capacity: NonZero<u32>,
        token: &region::Token<region::Identity<'d>>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            queue: token.bind(lanes::Fifo::try_from(capacity)?),
            route: PhantomData,
        })
    }

    pub(in crate::client) fn has_ready(&self, token: &region::Token<region::Identity<'d>>) -> bool {
        tagged::Endpoint::constant::<0>(token.borrow(&self.queue))
            .front()
            .is_some()
    }
}

impl<'d, ID: Identity> ReadyQueue<'d, ID, ()> for FixedReady<'d, ID> {
    /// The directory owns the generation; this queue retains only membership.
    /// Retirement removes membership before that directory slot can be reused.
    fn push_back<S>(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        bound: &Positioned<'_, '_, 'd, ID, (), S>,
    ) -> Result<(), entry::Error<NonZero<u32>>> {
        let queue = token.borrow_mut(&self.queue);
        if let Some(vacant) = queue.entry::<0>(bound.connection().index().into_usize())? {
            vacant.insert::<false>(()).commit();
        }
        Ok(())
    }

    fn remove<S>(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        bound: &Positioned<'_, '_, 'd, ID, (), S>,
    ) {
        token
            .borrow_mut(&self.queue)
            .remove_if(bound.connection().index().into_usize(), |()| true);
    }

    fn pop_front(&self, token: &mut region::Token<region::Identity<'d>>, (): ()) -> Option<usize> {
        tagged::Endpoint::constant::<0>(token.borrow_mut(&self.queue))
            .pop_entry()
            .map(|(index, ())| index)
    }

    fn front(&self, token: &mut region::Token<region::Identity<'d>>, (): ()) -> Option<usize> {
        tagged::Endpoint::constant::<0>(token.borrow(&self.queue))
            .front()
            .map(|entry| entry.key().get())
    }
}

impl<'d, ID: Identity> PoolTag<'d, ID> for () {
    type Ready = FixedReady<'d, ID>;
    type Lanes = Fixed<1>;
    type Connections = [Queue<'d, Fixed<1>>; 1];

    fn wake(self, queue: &Queue<'d, Self::Lanes>) {
        queue.wake_one::<0>();
    }

    fn register<'target>(
        self,
        queue: &'target Queue<'d, Self::Lanes>,
        entry: &mut Registration<'target, 'd, Self::Lanes>,
        wake: &wake::Waker<'d>,
    ) -> Result<(), Error> {
        queue.try_register::<0>(entry, wake).map_err(Into::into)
    }
}
