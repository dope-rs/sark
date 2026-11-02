use std::convert::Infallible;

use o3::buffer::hold;
use sark_http::owner::cause;

use crate::life::{self, exchange};

pub mod observed;

pub trait Trace<C: Copy> {
    type Session;
    type TransportTag: Copy + Default;

    fn tag(&self, id: exchange::Id<C, u64>, charge: hold::Charge) -> Self::TransportTag;
    fn transport(
        &self,
        conn: C,
        tag: Self::TransportTag,
    ) -> Option<(exchange::Id<C, u64>, hold::Charge)>;

    fn open(&mut self, session: &mut Self::Session, conn: C) -> exchange::Id<C, u64>;
    fn current(&self, session: &Self::Session, conn: C) -> Option<exchange::Id<C, u64>>;
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
    );
    fn stage(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        charge: hold::Charge,
    );
    fn unstage(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        charge: hold::Charge,
    );
    fn accept(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        charge: hold::Charge,
    );
    fn release(&mut self, id: exchange::Id<C, u64>, charge: hold::Charge, cause: cause::Transport);
    fn close(&mut self, session: &mut Self::Session, conn: C);
}

pub trait Mode<C: Copy>: life::Mode<C, u64> {
    /// Physical tag erased to `()` when lifecycle observation is disabled.
    type Session: Default;
    type TransportTag: Copy + Default;
    type Trace: Trace<C, Session = Self::Session, TransportTag = Self::TransportTag>;

    fn trace(&self) -> Option<&Self::Trace>;
    fn trace_mut(&mut self) -> Option<&mut Self::Trace>;

    fn tag(&self, id: exchange::Id<C, u64>, charge: hold::Charge) -> Self::TransportTag {
        self.trace()
            .map_or_else(Self::TransportTag::default, |trace| trace.tag(id, charge))
    }

    fn transport(
        &self,
        conn: C,
        tag: Self::TransportTag,
    ) -> Option<(exchange::Id<C, u64>, hold::Charge)> {
        self.trace()?.transport(conn, tag)
    }

    fn open(&mut self, session: &mut Self::Session, conn: C) -> exchange::Id<C, u64> {
        let id = self.trace_mut().map_or_else(
            || exchange::Id::new(conn, 0),
            |trace| trace.open(session, conn),
        );
        self.observe(life::Event::ExchangeOpened(id));
        id
    }

    fn current(&self, session: &Self::Session, conn: C) -> Option<exchange::Id<C, u64>> {
        self.trace().map_or_else(
            || Some(exchange::Id::new(conn, 0)),
            |trace| trace.current(session, conn),
        )
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
        if let Some(trace) = self.trace_mut() {
            trace.commit(session, id, bytes, charge);
        }
    }

    fn commit_link(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        bytes: usize,
        charge: hold::Charge,
        linked: hold::Charge,
    ) {
        if let Some(trace) = self.trace_mut() {
            trace.commit_link(session, id, bytes, charge, linked);
        }
    }

    fn stage(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        charge: hold::Charge,
    ) {
        if let Some(trace) = self.trace_mut() {
            trace.stage(session, id, charge);
        }
    }

    fn unstage(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        charge: hold::Charge,
    ) {
        if let Some(trace) = self.trace_mut() {
            trace.unstage(session, id, charge);
        }
    }

    fn accept(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<C, u64>,
        charge: hold::Charge,
    ) {
        if let Some(trace) = self.trace_mut() {
            trace.accept(session, id, charge);
        }
    }

    fn release(&mut self, id: exchange::Id<C, u64>, charge: hold::Charge, cause: cause::Transport) {
        if let Some(trace) = self.trace_mut() {
            trace.release(id, charge, cause);
        }
    }

    fn close(&mut self, session: &mut Self::Session, conn: C) {
        if let Some(trace) = self.trace_mut() {
            trace.close(session, conn);
        }
    }
}

impl<C: Copy> Trace<C> for Infallible {
    type Session = ();
    type TransportTag = ();

    fn tag(&self, _: exchange::Id<C, u64>, _: hold::Charge) {
        match *self {}
    }

    fn transport(
        &self,
        _: C,
        _: Self::TransportTag,
    ) -> Option<(exchange::Id<C, u64>, hold::Charge)> {
        match *self {}
    }

    fn open(&mut self, _: &mut Self::Session, _: C) -> exchange::Id<C, u64> {
        match *self {}
    }
    fn current(&self, _: &Self::Session, _: C) -> Option<exchange::Id<C, u64>> {
        match *self {}
    }
    fn commit_link(
        &mut self,
        _: &mut Self::Session,
        _: exchange::Id<C, u64>,
        _: usize,
        _: hold::Charge,
        _: hold::Charge,
    ) {
        match *self {}
    }

    fn stage(&mut self, _: &mut Self::Session, _: exchange::Id<C, u64>, _: hold::Charge) {
        match *self {}
    }
    fn unstage(&mut self, _: &mut Self::Session, _: exchange::Id<C, u64>, _: hold::Charge) {
        match *self {}
    }
    fn accept(&mut self, _: &mut Self::Session, _: exchange::Id<C, u64>, _: hold::Charge) {
        match *self {}
    }

    fn release(&mut self, _: exchange::Id<C, u64>, _: hold::Charge, _: cause::Transport) {
        match *self {}
    }

    fn close(&mut self, _: &mut Self::Session, _: C) {
        match *self {}
    }
}
