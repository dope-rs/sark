use std::marker::PhantomData;

use dope::{
    core::driver::{
        route::KeyTag,
        settings,
        settings::{claim, claim::Claim, errors},
        storage,
    },
    manifold::{app::creation, client::attempt::queue::mailbox},
    net::tcp,
};
use o3::depot::capacity;

pub struct Mailbox<O> {
    capacity: capacity::Count,
    outcome: PhantomData<fn() -> O>,
}

impl<O> Mailbox<O> {
    pub const fn new(capacity: capacity::Count) -> Self {
        Self {
            capacity,
            outcome: PhantomData,
        }
    }
}

impl<O> settings::Resource for Mailbox<O> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(Claim::new(settings::Routes::singleton::<KeyTag<0>>()))
    }
}

impl<O> storage::Plan for Mailbox<O> {
    type Output<'p, 'd>
        = mailbox::Domain<'d, tcp::Tcp, u64, O>
    where
        Self: 'd,
        'd: 'p;
}

impl<O> storage::Create for Mailbox<O> {
    type Error = creation::Error;

    fn try_build<'p, 'd>(
        self,
        mut context: storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        mailbox::Domain::try_new(self.capacity, &mut context)
    }
}
