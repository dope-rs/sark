use o3::buffer::hold;
use sark_http::owner::cause;

use crate::life::{self, exchange};

pub mod observed;

pub trait Mode<C: Copy>: life::Mode<C, u64> {
    /// Physical tag erased to `()` when lifecycle observation is disabled.
    type Session: Default;
    type TransportTag: Copy + Default;

    fn tag(&self, _: exchange::Id<C, u64>, _: hold::Charge) -> Self::TransportTag {
        Self::TransportTag::default()
    }

    fn transport(
        &self,
        _: C,
        _: Self::TransportTag,
    ) -> Option<(exchange::Id<C, u64>, hold::Charge)> {
        None
    }

    fn next(&mut self, _: &mut Self::Session, conn: C) -> exchange::Id<C, u64> {
        exchange::Id::new(conn, 0)
    }

    fn open(&mut self, session: &mut Self::Session, conn: C) -> exchange::Id<C, u64> {
        let id = self.next(session, conn);
        self.observe(life::Event::ExchangeOpened(id));
        id
    }

    fn current(&self, _: &Self::Session, conn: C) -> Option<exchange::Id<C, u64>> {
        Some(exchange::Id::new(conn, 0))
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
        _: &mut Self::Session,
        _: exchange::Id<C, u64>,
        _: usize,
        _: hold::Charge,
        _: hold::Charge,
    ) {
    }

    fn stage(&mut self, _: &mut Self::Session, _: exchange::Id<C, u64>, _: hold::Charge) {}

    fn unstage(&mut self, _: &mut Self::Session, _: exchange::Id<C, u64>, _: hold::Charge) {}

    fn accept(&mut self, _: &mut Self::Session, _: exchange::Id<C, u64>, _: hold::Charge) {}

    fn release(&mut self, _: exchange::Id<C, u64>, _: hold::Charge, _: cause::Transport) {}

    fn close(&mut self, _: &mut Self::Session, _: C) {}
}
