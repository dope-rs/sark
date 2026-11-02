//! Enabled lifecycle observation.

use o3::buffer::hold::{self, evidence};
use sark_http::owner::{self, cause};
use sequence::observed::{Session, Tag};

use crate::life::{self, endpoint, exchange, sequence};

#[repr(transparent)]
#[derive(Clone, Copy, Default)]
pub struct Observed<O>(O);

impl<O> Observed<O> {
    pub const fn new(observer: O) -> Self {
        Self(observer)
    }

    pub const fn observer(&self) -> &O {
        &self.0
    }

    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.0
    }

    pub fn into_inner(self) -> O {
        self.0
    }
}

impl<E, T, O> endpoint::Mode<E, T> for Observed<O>
where
    O: endpoint::Observer<E, T>,
{
    const ENABLED: bool = true;

    type Observer = O;

    fn observer(&mut self) -> Option<&mut Self::Observer> {
        Some(&mut self.0)
    }
}

impl<C: Copy, S: Copy, O> life::Mode<C, S> for Observed<O>
where
    O: life::Observer<C, S>,
{
    const ENABLED: bool = true;

    type Retention = evidence::On;
    type Observer = O;

    type Body<'a>
        = life::BodyScope<'a, C, S, O>
    where
        Self: 'a,
        C: 'a,
        S: 'a;

    fn observer(&mut self) -> Option<&mut Self::Observer> {
        Some(&mut self.0)
    }

    fn body<'a>(&'a mut self, id: exchange::Id<C, S>) -> Self::Body<'a>
    where
        C: 'a,
        S: 'a,
    {
        life::BodyScope {
            id,
            out: &mut self.0,
        }
    }
}

impl<C, O> sequence::Mode<C> for Observed<O>
where
    C: Copy,
    O: life::Observer<C, u64>,
{
    type Session = Session;
    type TransportTag = Tag;
    type Trace = Self;

    fn trace(&self) -> Option<&Self::Trace> {
        Some(self)
    }

    fn trace_mut(&mut self) -> Option<&mut Self::Trace> {
        Some(self)
    }
}

impl<C, O> sequence::Trace<C> for Observed<O>
where
    C: Copy,
    O: life::Observer<C, u64>,
{
    type Session = Session;
    type TransportTag = Tag;

    fn tag(&self, id: exchange::Id<C, u64>, charge: hold::Charge) -> Self::TransportTag {
        Tag::new(id.stream(), charge)
    }

    fn transport(
        &self,
        conn: C,
        tag: Self::TransportTag,
    ) -> Option<(exchange::Id<C, u64>, hold::Charge)> {
        tag.parts()
            .map(|(order, charge)| (exchange::Id::new(conn, order), charge))
    }

    fn open(&mut self, session: &mut Self::Session, conn: C) -> exchange::Id<C, u64> {
        exchange::Id::new(conn, session.advance())
    }

    fn current(&self, session: &Self::Session, conn: C) -> Option<exchange::Id<C, u64>> {
        session
            .current()
            .map(|order| exchange::Id::new(conn, order))
    }

    fn commit_link(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        bytes: usize,
        charge: hold::Charge,
        linked: hold::Charge,
    ) {
        self.0.observe(life::Event::ResponseCommitted {
            exchange: id,
            body_bytes: bytes,
            charge,
            linked,
        });
        session.stage(id.stream(), charge);
    }

    fn stage(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        charge: hold::Charge,
    ) {
        if charge.is_empty() {
            return;
        }
        session.stage(id.stream(), charge);
        self.0
            .observe(life::Event::ResponseOwnership(owner::Effect::new(
                id,
                charge,
                owner::Change::StagingAcquired(cause::Acquire::StreamChunk),
            )));
    }

    fn unstage(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        charge: hold::Charge,
    ) {
        if charge.is_empty() {
            return;
        }
        session.take();
        self.0
            .observe(life::Event::ResponseOwnership(owner::Effect::new(
                id,
                charge,
                owner::Change::StagingReleased(cause::Staging::Closed),
            )));
    }

    fn accept(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        charge: hold::Charge,
    ) {
        if charge.is_empty() {
            return;
        }
        session.take();
        self.0
            .observe(life::Event::ResponseOwnership(owner::Effect::new(
                id,
                charge,
                owner::Change::Handoff,
            )));
    }

    fn release(&mut self, id: exchange::Id<C, u64>, charge: hold::Charge, cause: cause::Transport) {
        self.0
            .observe(life::Event::ResponseOwnership(owner::Effect::new(
                id,
                charge,
                owner::Change::TransportReleased(cause),
            )));
    }

    fn close(&mut self, session: &mut Self::Session, conn: C) {
        let Some((stream, charge)) = session.close() else {
            return;
        };
        self.0
            .observe(life::Event::ResponseOwnership(owner::Effect::new(
                exchange::Id::new(conn, stream),
                charge,
                owner::Change::StagingReleased(cause::Staging::Closed),
            )));
    }
}

const _: () = assert!(core::mem::size_of::<Observed<()>>() == 0);
