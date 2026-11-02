//! Static selection of an input lease or finite exchange deadlines.

use std::time;

use dope::{
    core::driver::{
        route::Identity,
        schedule::{clock, ready::task::owner},
    },
    manifold::network::listener::conn,
};
use sark_core::{
    deadline::config,
    life::{self, exchange, sequence},
};

use crate::server::deadline::{self, domain, view};

/// Existing zero-state policy: only an incomplete request head owns the root.
#[derive(Clone, Copy, Default)]
pub struct HeadOnly;

/// One absolute input window covering the request head and body.
/// Progress and pipelined input do not renew an already armed window.
#[derive(Clone, Copy, Default)]
pub struct Input;

/// Bound input-lease policy with no deadline projection state.
#[doc(hidden)]
#[repr(transparent)]
pub struct Head<M, const BODY: bool = false>(M);

impl<M, const BODY: bool> Head<M, BODY> {
    const fn new(mode: M) -> Self {
        Self(mode)
    }
}

/// Binds one compile-time deadline policy to a listener-capacity state domain.
#[doc(hidden)]
pub trait Policy<'d, ID: Identity>: Copy {
    type Mode<M>: deadline::Mode<
            'd,
            ID,
            Retention = M::Retention,
            Session = M::Session,
            TransportTag = M::TransportTag,
        >
    where
        M: sequence::Mode<conn::Id<'d, ID>>;

    fn bind<M>(self, mode: M, cap: owner::Capacity) -> Result<Self::Mode<M>, deadline::BindError>
    where
        M: sequence::Mode<conn::Id<'d, ID>>;
    fn timeout(self, head: time::Duration) -> Option<time::Duration> {
        Some(head)
    }
}

impl<'d, ID: Identity> Policy<'d, ID> for HeadOnly {
    type Mode<M>
        = Head<M>
    where
        M: sequence::Mode<conn::Id<'d, ID>>;

    fn bind<M>(self, mode: M, _: owner::Capacity) -> Result<Self::Mode<M>, deadline::BindError>
    where
        M: sequence::Mode<conn::Id<'d, ID>>,
    {
        Ok(Head::new(mode))
    }
}

impl<'d, ID: Identity> Policy<'d, ID> for Input {
    type Mode<M>
        = Head<M, true>
    where
        M: sequence::Mode<conn::Id<'d, ID>>;

    fn bind<M>(self, mode: M, _: owner::Capacity) -> Result<Self::Mode<M>, deadline::BindError>
    where
        M: sequence::Mode<conn::Id<'d, ID>>,
    {
        Ok(Head::new(mode))
    }
}

impl<'d, ID: Identity> Policy<'d, ID> for config::Config {
    type Mode<M>
        = view::View<'d, M, ID>
    where
        M: sequence::Mode<conn::Id<'d, ID>>;

    fn bind<M>(self, mode: M, cap: owner::Capacity) -> Result<Self::Mode<M>, deadline::BindError>
    where
        M: sequence::Mode<conn::Id<'d, ID>>,
    {
        let state = domain::Domain::try_new(self, cap).map_err(deadline::BindError::Allocation)?;
        Ok(view::View::new(mode, state))
    }

    fn timeout(self, _: time::Duration) -> Option<time::Duration> {
        None
    }
}

impl<'d, ID: Identity, M, const BODY: bool> deadline::Mode<'d, ID> for Head<M, BODY>
where
    M: sequence::Mode<conn::Id<'d, ID>>,
{
    fn begin(&mut self, now: clock::Moment<'d>) -> clock::Moment<'d> {
        now
    }

    fn finish(
        &mut self,
        _: conn::Id<'d, ID>,
        _: clock::Moment<'d>,
        head: deadline::Head,
        _: deadline::Body,
    ) -> deadline::Root<clock::Deadline<'d>> {
        match head {
            deadline::Head::Pending => deadline::Root::Ensure,
            deadline::Head::Body if BODY => deadline::Root::Ensure,
            deadline::Head::Ready | deadline::Head::Body => deadline::Root::Cancel,
            deadline::Head::Unchanged => deadline::Root::Unchanged,
        }
    }

    fn timeout(
        &mut self,
        _: conn::Id<'d, ID>,
        _: clock::Moment<'d>,
    ) -> deadline::Due<conn::Id<'d, ID>, clock::Deadline<'d>> {
        deadline::Due {
            expiry: Some(deadline::Expiry::Connection),
            root: deadline::Root::Unchanged,
        }
    }
}

impl<'d, ID: Identity, M, const BODY: bool> life::Mode<conn::Id<'d, ID>, u64> for Head<M, BODY>
where
    M: life::Mode<conn::Id<'d, ID>, u64>,
{
    const ENABLED: bool = M::ENABLED;

    type Retention = M::Retention;
    type Observer = M::Observer;

    type Body<'a>
        = M::Body<'a>
    where
        Self: 'a,
        conn::Id<'d, ID>: 'a;

    fn observer(&mut self) -> Option<&mut Self::Observer> {
        self.0.observer()
    }

    fn body<'a>(&'a mut self, id: exchange::Id<conn::Id<'d, ID>, u64>) -> Self::Body<'a>
    where
        conn::Id<'d, ID>: 'a,
    {
        self.0.body(id)
    }
}

impl<'d, ID: Identity, M, const BODY: bool> sequence::Mode<conn::Id<'d, ID>> for Head<M, BODY>
where
    M: sequence::Mode<conn::Id<'d, ID>>,
{
    type Session = M::Session;
    type TransportTag = M::TransportTag;
    type Trace = M::Trace;

    fn trace(&self) -> Option<&Self::Trace> {
        self.0.trace()
    }

    fn trace_mut(&mut self) -> Option<&mut Self::Trace> {
        self.0.trace_mut()
    }
}

const _: () = assert!(
    core::mem::size_of::<Head<life::mode::Disabled>>()
        == core::mem::size_of::<life::mode::Disabled>()
);
