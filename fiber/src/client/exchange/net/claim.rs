use std::{convert::Infallible, mem};

use dope::{
    core::{
        driver,
        driver::route::{Identity, KeyTag},
    },
    manifold::client::{conn::Id, port},
};
use dope_fiber::reply::{self, recv::attach};
use net::pools::entries;
use o3::{base::cell::region, buffer::pool};
use sark_client::{Error, capacity, conn};

use crate::client::{
    config::report,
    exchange::{net, request},
};

pub(super) enum Acquire<T, D> {
    Ready(T),
    Pending,
    FailureReport,
    Failed(conn::Error<D>),
    Reply(attach::Error),
}

pub(super) enum Arm<T, D> {
    Ready(T),
    Missing,
    FailureReport(T),
    Failed(conn::Error<D>),
    Reply(attach::Error),
}

/// One connection owned for exactly one application poll.
#[repr(C)]
pub(in crate::client) struct Claim<
    's,
    'turn,
    'f,
    'e,
    'b,
    'c,
    'd,
    ID: Identity,
    Tag = (),
    E = Infallible,
    F = report::Never,
> where
    Tag: net::PoolTag<'d, ID>,
    F: report::Policy<E>,
    E: 'f,
{
    driver: driver::Context<'turn, 'd>,
    current: entries::Positioned<'s, 'b, 'd, ID, Tag, F::State<'f, 'd>>,
    owner: &'s net::Transport<'f, 'e, 'b, 'c, 'd, ID, Tag, E, F>,
}

impl<'s, 'turn, 'f, 'e, 'b, 'c, 'd, ID: Identity, Tag, E, F>
    Claim<'s, 'turn, 'f, 'e, 'b, 'c, 'd, ID, Tag, E, F>
where
    Tag: net::PoolTag<'d, ID>,
    F: report::Policy<E>,
    E: 'f,
{
    pub(in crate::client) fn try_acquire(
        owner: &'s net::Transport<'f, 'e, 'b, 'c, 'd, ID, Tag, E, F>,
        mut driver: driver::Context<'turn, 'd>,
        tag: Tag,
        io: &port::Port<'_, 'd, pool::Cursor, ID>,
    ) -> Result<Option<Self>, Error<F::Detail<'f>>> {
        match owner.pool.acquire(&mut driver, tag, io) {
            Acquire::Ready(current) => Ok(Some(Self {
                driver,
                current,
                owner,
            })),
            Acquire::Pending => Ok(None),
            Acquire::FailureReport => Err(Error::Capacity(capacity::Error::FailureReport)),
            Acquire::Failed(error) => Err(Error::Transport(error)),
            Acquire::Reply(error) => {
                owner.waiters.begin_drain();
                Err(Error::Reply(error))
            }
        }
    }

    pub(in crate::client) fn try_new(
        owner: &'s net::Transport<'f, 'e, 'b, 'c, 'd, ID, Tag, E, F>,
        mut driver: driver::Context<'turn, 'd>,
        conn: Id<'d, ID>,
    ) -> Result<Self, Error<F::Detail<'f>>> {
        match owner.pool.arm(driver.region_token(), conn) {
            Arm::Ready(current) => Ok(Self {
                driver,
                current,
                owner,
            }),
            Arm::Missing => Err(Error::NotConnected),
            Arm::FailureReport(current) => {
                Self {
                    driver,
                    current,
                    owner,
                }
                .release();
                Err(Error::Capacity(capacity::Error::FailureReport))
            }
            Arm::Failed(error) => Err(Error::Transport(error)),
            Arm::Reply(error) => {
                owner.waiters.begin_drain();
                Err(Error::Reply(error))
            }
        }
    }

    pub(in crate::client) fn connection(&self) -> Id<'d, ID> {
        self.current.connection()
    }

    pub(in crate::client) fn lane(
        &self,
    ) -> reply::Lane<
        's,
        'b,
        'c,
        'd,
        net::Outcome<'d, F::Detail<'f>>,
        net::Outcome<'d, F::Detail<'f>>,
        request::Kind,
    > {
        self.owner.pool.responses.lane(self.current.position())
    }

    pub(in crate::client) fn region(&mut self) -> &mut region::Token<region::Identity<'d>> {
        self.driver.region_token()
    }

    pub(in crate::client) fn commit(self) {
        let Self {
            mut driver,
            current,
            owner,
        } = self;
        if let Some(tag) = owner
            .pool
            .submit(driver.region_token(), current, owner.idle_policy)
        {
            owner.waiters.wake(tag);
        }
    }

    pub(in crate::client) fn release(self) {
        let Self {
            mut driver,
            current,
            owner,
        } = self;
        if let Some(tag) = owner.pool.release(&mut driver, current, owner.idle_policy) {
            owner.waiters.wake(tag);
        }
    }

    pub(in crate::client) fn retire(self) {
        let Self {
            mut driver,
            current,
            owner,
        } = self;
        owner.pool.close_claim(driver.region_token(), current);
    }
}

const _: () = {
    assert!(
        mem::size_of::<entries::Positioned<'static, 'static, 'static, KeyTag<0>, (), ()>>()
            == mem::size_of::<&()>() + mem::size_of::<Id<'static, KeyTag<0>>>()
    );
    assert!(
        mem::size_of::<Option<entries::Positioned<'static, 'static, 'static, KeyTag<0>, (), ()>>>()
            == mem::size_of::<entries::Positioned<'static, 'static, 'static, KeyTag<0>, (), ()>>()
    );
    assert!(
        mem::size_of::<
            Claim<'static, 'static, 'static, 'static, 'static, 'static, 'static, KeyTag<0>>,
        >() == mem::size_of::<driver::Context<'static, 'static>>()
            + mem::size_of::<entries::Positioned<'static, 'static, 'static, KeyTag<0>, (), ()>>()
            + mem::size_of::<&net::Transport<'static, 'static, 'static, 'static, 'static, KeyTag<0>>>(
            )
    );
};
