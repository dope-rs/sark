//! Static selection of an input lease or finite exchange deadlines.

use std::{fmt, time};

use dope::{
    core::driver::{
        route::Identity,
        schedule::{clock, ready::task::owner},
    },
    manifold::network::listener::conn,
};
use o3::buffer::hold;
use sark_http::owner::cause;

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
        Ok(Head(mode))
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
        Ok(Head(mode))
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

    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E) {
        self.0.diagnostic(cause);
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
    fn tag(
        &self,
        id: exchange::Id<conn::Id<'d, ID>, u64>,
        charge: hold::Charge,
    ) -> Self::TransportTag {
        sequence::Mode::tag(&self.0, id, charge)
    }

    fn transport(
        &self,
        conn: conn::Id<'d, ID>,
        tag: Self::TransportTag,
    ) -> Option<(exchange::Id<conn::Id<'d, ID>, u64>, hold::Charge)> {
        sequence::Mode::transport(&self.0, conn, tag)
    }

    fn next(
        &mut self,
        session: &mut Self::Session,
        conn: conn::Id<'d, ID>,
    ) -> exchange::Id<conn::Id<'d, ID>, u64> {
        sequence::Mode::next(&mut self.0, session, conn)
    }

    fn current(
        &self,
        session: &Self::Session,
        conn: conn::Id<'d, ID>,
    ) -> Option<exchange::Id<conn::Id<'d, ID>, u64>> {
        sequence::Mode::current(&self.0, session, conn)
    }

    fn commit(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<conn::Id<'d, ID>, u64>,
        bytes: usize,
        charge: hold::Charge,
    ) {
        sequence::Mode::commit(&mut self.0, session, id, bytes, charge)
    }

    fn commit_link(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<conn::Id<'d, ID>, u64>,
        bytes: usize,
        charge: hold::Charge,
        linked: hold::Charge,
    ) {
        sequence::Mode::commit_link(&mut self.0, session, id, bytes, charge, linked)
    }

    fn stage(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<conn::Id<'d, ID>, u64>,
        charge: hold::Charge,
    ) {
        sequence::Mode::stage(&mut self.0, session, id, charge)
    }

    fn unstage(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<conn::Id<'d, ID>, u64>,
        charge: hold::Charge,
    ) {
        sequence::Mode::unstage(&mut self.0, session, id, charge)
    }

    fn accept(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<conn::Id<'d, ID>, u64>,
        charge: hold::Charge,
    ) {
        sequence::Mode::accept(&mut self.0, session, id, charge)
    }

    fn release(
        &mut self,
        id: exchange::Id<conn::Id<'d, ID>, u64>,
        charge: hold::Charge,
        cause: cause::Transport,
    ) {
        sequence::Mode::release(&mut self.0, id, charge, cause)
    }

    fn close(&mut self, session: &mut Self::Session, conn: conn::Id<'d, ID>) {
        sequence::Mode::close(&mut self.0, session, conn)
    }
}

const _: () = assert!(
    core::mem::size_of::<Head<life::mode::Disabled>>()
        == core::mem::size_of::<life::mode::Disabled>()
);
