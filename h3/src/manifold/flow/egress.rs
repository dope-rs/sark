use conn::scope;
use dope_quic::{
    conn, stream,
    wire::stream::{Direction, Id},
};
use o3::buffer::resident;
use sark_core::life::{self, mode};
use sark_http::{self, field::metadata::section::rules};

use crate::{
    link::Flush,
    manifold::{self, flow, flow::report},
};

/// Exclusive egress authority borrowed from one HTTP/3 session.
#[repr(transparent)]
pub struct Egress<'session, 'd, P: rules::Policy> {
    session: &'session mut manifold::Session<'d, P>,
}

impl<'session, 'd, P: rules::Policy> Egress<'session, 'd, P> {
    pub(in crate::manifold) const fn new(sess: &'session mut manifold::Session<'d, P>) -> Self {
        Self { session: sess }
    }

    pub fn start<const ID: u8, B>(
        &mut self,
        quic: &mut conn::Connection<ID, B, resident::Shared<'d>>,
    ) -> Result<Id, manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        quic.with_scope(|mut scope| self.start_scope(&mut scope))
    }

    /// Opens the required local H3 streams through an existing QUIC scope.
    pub fn start_scope<const ID: u8, B, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
    ) -> Result<Id, manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        Ok(self
            .start_as::<ID, B, (), mode::Disabled, RECONCILE>(quic, &mut mode::Disabled, ())?
            .0)
    }

    pub(in crate::manifold) fn start_with<
        const ID: u8,
        B,
        C: Copy,
        O: life::Mode<C, Id>,
        const RECONCILE: bool,
    >(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        life: &mut O,
        handle: C,
    ) -> Result<report::Report, manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        self.start_as::<ID, B, C, O, RECONCILE>(quic, life, handle)
            .map(|started| started.1)
    }

    fn start_as<const ID: u8, B, C: Copy, O: life::Mode<C, Id>, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        life: &mut O,
        handle: C,
    ) -> Result<(Id, report::Report), manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        self.session.validate(quic)?;
        let ctrl = {
            let mut table = quic.streams();
            table.send().open_handle(Direction::Uni)
        }?;
        let id = ctrl.id();
        self.session
            .h3
            .control()
            .open(manifold::resident_target(ctrl))?;
        let mut out = self.settle_as::<ID, B, C, O, RECONCILE>(quic, life, handle)?;
        if self.session.h3.settings().local().table().get() > 0 {
            let dec = {
                let mut table = quic.streams();
                table.send().open_handle(Direction::Uni)
            }?;
            self.session
                .h3
                .control()
                .open_decoder(manifold::resident_target(dec))?;
            let next = self.settle_as::<ID, B, C, O, RECONCILE>(quic, life, handle)?;
            out = out.merged(next);
        }
        Ok((id, out))
    }

    pub fn open<const ID: u8, B>(
        &mut self,
        quic: &mut conn::Connection<ID, B, resident::Shared<'d>>,
    ) -> Result<crate::stream::Owned, manifold::Error>
    where
        B: stream::Recv,
    {
        quic.with_scope(|mut scope| self.open_scope(&mut scope))
    }

    /// Opens one application stream through an existing QUIC scope.
    pub fn open_scope<const ID: u8, B: stream::Recv, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
    ) -> Result<crate::stream::Owned, manifold::Error> {
        let origin = self.session.bind_scope(quic)?.origin();
        self.session.h3.admit()?;
        let mut table = quic.streams();
        let handle = table.send().open_handle(Direction::Bidi)?;
        table.binding().try_attach(handle)?;
        Ok(crate::stream::Owned::new(origin, handle.local()))
    }

    pub fn flush<const ID: u8, B>(
        &mut self,
        quic: &mut conn::Connection<ID, B, resident::Shared<'d>>,
    ) -> Result<Flush, manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        quic.with_scope(|mut scope| self.flush_scope(&mut scope))
    }

    /// Pumps local batches until no unscheduled write work remains.
    pub fn settle<const ID: u8, B>(
        &mut self,
        quic: &mut conn::Connection<ID, B, resident::Shared<'d>>,
    ) -> Result<Flush, manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        quic.with_scope(|mut scope| self.settle_scope(&mut scope))
    }

    /// Flushes one bounded write turn through an existing QUIC scope.
    pub fn flush_scope<const ID: u8, B, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
    ) -> Result<Flush, manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        Ok(self.flush_with(quic, &mut mode::Disabled, ())?.flush())
    }

    /// Settles all scheduled writes, reporting only the flush state.
    /// Request queues use [`Self::flush_with`] to account for pending releases.
    pub fn settle_scope<const ID: u8, B, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
    ) -> Result<Flush, manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        let report =
            self.settle_as::<ID, B, (), mode::Disabled, RECONCILE>(quic, &mut mode::Disabled, ())?;
        Ok(report.flush())
    }

    pub(in crate::manifold) fn flush_with<
        const ID: u8,
        B,
        C: Copy,
        O: life::Mode<C, Id>,
        const RECONCILE: bool,
    >(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        life: &mut O,
        handle: C,
    ) -> Result<report::Report, manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        let owner = self.session.bind_scope(quic)?.owner();
        let mut net = flow::Transport::new(quic, &mut self.session.receive_credits, owner, None);
        net.pump::<P, C, O>(&mut self.session.h3, life, handle)
    }

    pub(super) fn settle_as<const ID: u8, B, C: Copy, O: life::Mode<C, Id>, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        life: &mut O,
        handle: C,
    ) -> Result<report::Report, manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        let owner = self.session.bind_scope(quic)?.owner();
        let mut net = flow::Transport::new(quic, &mut self.session.receive_credits, owner, None);
        let mut released = false;
        loop {
            let report = net.pump::<P, C, O>(&mut self.session.h3, life, handle)?;
            released |= report.has_release();
            if report.flush() != Flush::Pending {
                return Ok(report::Report::new(report.flush(), released));
            }
        }
    }
}
