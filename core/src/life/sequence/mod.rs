use o3::buffer::hold;
use sark_http::owner::{self, cause};

use crate::life::{self, exchange};

pub mod observed;

pub trait Session<K: Copy + Default>: Default {
    fn tag(_: u64, _: hold::Charge) -> K {
        K::default()
    }

    fn parts(_: K) -> Option<(u64, hold::Charge)> {
        None
    }

    fn advance(&mut self) -> u64 {
        0
    }

    fn current(&self) -> Option<u64> {
        Some(0)
    }

    fn stage(&mut self, _: u64, _: hold::Charge) -> bool {
        false
    }

    fn clear_staged(&mut self, _: hold::Charge) -> bool {
        false
    }

    fn close(&mut self) -> Option<(u64, hold::Charge)> {
        None
    }
}

impl Session<()> for () {}

pub trait Mode<C: Copy>: life::Mode<C, u64> {
    type Session: Session<Self::TransportTag>;
    type TransportTag: Copy + Default;

    fn tag(&self, id: exchange::Id<C, u64>, charge: hold::Charge) -> Self::TransportTag {
        Self::Session::tag(id.stream(), charge)
    }

    fn transport(
        &self,
        conn: C,
        tag: Self::TransportTag,
    ) -> Option<(exchange::Id<C, u64>, hold::Charge)> {
        Self::Session::parts(tag).map(|(order, charge)| (exchange::Id::new(conn, order), charge))
    }

    fn open(&mut self, session: &mut Self::Session, conn: C) -> exchange::Id<C, u64> {
        let id = exchange::Id::new(conn, session.advance());
        self.observe(life::Event::ExchangeOpened(id));
        id
    }

    fn current(&self, session: &Self::Session, conn: C) -> Option<exchange::Id<C, u64>> {
        session
            .current()
            .map(|order| exchange::Id::new(conn, order))
    }

    fn reject(&mut self, id: exchange::Id<C, u64>)
    where
        Self: Sized,
    {
        id.retire(self, life::Outcome::Rejected);
    }

    fn commit(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        bytes: usize,
        charge: hold::Charge,
    ) {
        self.commit_link(session, id, bytes, charge, hold::Charge::default());
    }

    fn commit_link(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        bytes: usize,
        charge: hold::Charge,
        linked: hold::Charge,
    ) {
        self.observe(life::Event::ResponseCommitted {
            exchange: id,
            body_bytes: bytes,
            charge,
            linked,
        });
        session.stage(id.stream(), charge);
    }

    fn ownership(&mut self, effect: owner::Effect<exchange::Id<C, u64>>) {
        self.observe(life::Event::ResponseOwnership(effect));
    }

    fn stage(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        charge: hold::Charge,
    ) {
        if session.stage(id.stream(), charge) {
            self.ownership(owner::Effect::new(
                id,
                charge,
                owner::Change::StagingAcquired(cause::Acquire::StreamChunk),
            ));
        }
    }

    fn unstage(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        charge: hold::Charge,
    ) {
        if session.clear_staged(charge) {
            self.ownership(owner::Effect::new(
                id,
                charge,
                owner::Change::StagingReleased(cause::Staging::Closed),
            ));
        }
    }

    fn accept(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        charge: hold::Charge,
    ) {
        if session.clear_staged(charge) {
            self.ownership(owner::Effect::new(id, charge, owner::Change::Handoff));
        }
    }

    fn release(&mut self, id: exchange::Id<C, u64>, charge: hold::Charge, cause: cause::Transport) {
        self.ownership(owner::Effect::new(
            id,
            charge,
            owner::Change::TransportReleased(cause),
        ));
    }

    fn close(&mut self, session: &mut Self::Session, conn: C) {
        if let Some((order, charge)) = session.close() {
            self.ownership(owner::Effect::new(
                exchange::Id::new(conn, order),
                charge,
                owner::Change::StagingReleased(cause::Staging::Closed),
            ));
        }
    }
}
