//! Statically selected H3 lifecycle and owner projection.
use std::convert::Infallible;

use dope_quic::{
    conn::{
        id, scope,
        stream::{Error, resident},
    },
    errors, stream,
    wire::stream::{Id, Kind},
};
use o3::buffer::{
    hold::{
        self,
        evidence::{Mode, Off, On},
    },
    resident::Shared,
};
use resident::Local;
use sark_core::{
    life::{self, Observer as _, audit, exchange, mode, owners},
    manifold::task::response,
};
use sark_http::{
    field::metadata::section::rules,
    owner::{self, cause, evidence, handoff::stage},
};
use send::admit;
use stream::{release, send};

use crate::{
    conn, frame,
    manifold::{self, Diagnostic, encoder},
};

/// Owner-bound capability for projecting one H3 lifecycle effect.
#[repr(C)]
pub struct Projection<'a, O, C> {
    observer: &'a mut O,
    handle: C,
}

/// All H3 session roots have been destroyed and their release callbacks completed.
/// This excludes request credit and payload aliases transferred to QUIC.
#[must_use]
pub(in crate::manifold) struct Closed(());

impl Closed {
    pub(in crate::manifold) fn apply(self, snapshot: audit::Snapshot) -> audit::Snapshot {
        snapshot.conn_ready(0).h3_bytes(0)
    }
}

impl<'a, O, C> Projection<'a, O, C> {
    pub const fn new(observer: &'a mut O, handle: C) -> Self {
        Self { observer, handle }
    }
}

impl<C: Copy, O: life::Mode<C, Id>> Projection<'_, O, C> {
    pub(in crate::manifold) fn close<P: rules::Policy>(
        self,
        session: manifold::Session<'_, P>,
    ) -> Closed {
        if O::ENABLED {
            session.close(|effect| Projection::new(self.observer, self.handle).unstage(effect));
        } else {
            drop(session);
        }
        Closed(())
    }

    pub fn released(&mut self, event: release::Event) {
        let id = event.source.id();
        if id.kind() != Kind::ClientBidi {
            return;
        }
        let cause = match event.cause {
            release::Cause::Acked => cause::Transport::Acked,
            release::Cause::Stopped => cause::Transport::Stopped,
            release::Cause::Reset => cause::Transport::Reset,
            release::Cause::Closed => cause::Transport::Closed,
            release::Cause::PeerRetired => cause::Transport::PeerRetired,
        };
        owners::Projection::new(self.observer).apply(owner::Effect::new(
            exchange::Id::new(self.handle, id),
            event.charge,
            owner::Change::TransportReleased(cause),
        ));
    }

    pub(in crate::manifold) fn admitted(self, charge: hold::Charge, id: Id) {
        if !O::ENABLED || id.kind() != Kind::ClientBidi {
            return;
        }
        let mut sink = owners::Sink::new(self.observer, self.handle);
        sink.apply(owner::Effect::new(id, charge, owner::Change::Handoff));
    }

    pub(in crate::manifold) fn discarded(self, charge: hold::Charge, id: Id, gone: admit::Cause) {
        if !O::ENABLED {
            return;
        }
        let cause = match gone {
            admit::Cause::PeerRetired => cause::Staging::PeerRetired,
            admit::Cause::Reset => cause::Staging::Reset,
            admit::Cause::Finished | admit::Cause::Retired => cause::Staging::Closed,
        };
        self.unstage(owner::Effect::new(
            id,
            charge,
            owner::Change::StagingReleased(cause),
        ));
    }

    pub(in crate::manifold) fn unstage(self, effect: owner::Effect<Id>) {
        if !O::ENABLED {
            return;
        }
        if effect.key().kind() == Kind::ClientBidi {
            let mut sink = owners::Sink::new(self.observer, self.handle);
            sink.apply(effect);
        }
    }
}

impl<C: Copy, O: life::Mode<C, Id>> release::Sink for Projection<'_, O, C>
where
    O::Retention: Handoff,
{
    type Mode = O::Retention;

    fn release(
        &mut self,
        proof: <O::Retention as Mode>::Projection,
        source: admit::Source,
        gone: release::Cause,
    ) {
        let charge = <O::Retention as Handoff>::charge(proof);
        self.released(release::Event {
            source,
            charge,
            cause: gone,
        });
    }
}

const _: () = assert!(
    core::mem::size_of::<Projection<'static, mode::Disabled, u64>>()
        == core::mem::size_of::<(&mut mode::Disabled, u64)>()
);

enum Reset {
    Foreign(resident::Foreign),
    Stale(resident::Stale),
    Stream(Error),
}

impl Reset {
    fn code<C: Copy, O: life::Mode<C, Id>>(
        self,
        view: &mut Projection<'_, O, C>,
    ) -> frame::ErrorCode {
        if let Some(observer) = view.observer.observer() {
            let cause = match &self {
                Self::Foreign(error) => Diagnostic::ResetForeign(error),
                Self::Stale(error) => Diagnostic::ResetStale(error),
                Self::Stream(error) => Diagnostic::ResetStream(error),
            };
            observer.diagnostic(&cause);
        }
        match self {
            Self::Foreign(_) | Self::Stale(_) | Self::Stream(_) => frame::ErrorCode::Internal,
        }
    }
}

impl From<resident::Error> for Reset {
    fn from(error: resident::Error) -> Self {
        match error {
            resident::Error::Stream(error) => Self::Stream(error),
            resident::Error::Stale(error) => Self::Stale(error),
        }
    }
}

impl<'a, 'd, const ID: u8, O> Projection<'a, O, id::Handle<id::Driver<'d>, ID>>
where
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    O::Retention: Handoff,
{
    pub(in crate::manifold) fn reset_bound<'conn, B: stream::Recv, const RECONCILE: bool>(
        mut self,
        quic: &mut scope::Access<'conn, ID, B, RECONCILE, Shared<'d>>,
        handle: resident::Handle<'conn>,
        code: frame::ErrorCode,
    ) {
        let code = errors::Code::from_u16(code.wire_code());
        if let Err(error) = quic.streams().try_reset(handle, code, &mut self) {
            manifold::close_connection_scoped(quic, Reset::from(error).code(&mut self));
        }
    }

    pub(in crate::manifold) fn reset<B: stream::Recv, const RECONCILE: bool>(
        mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, Shared<'d>>,
        target: crate::stream::Target,
        code: frame::ErrorCode,
    ) {
        if let Err(error) = self.try_reset(quic, target, code) {
            manifold::close_connection_scoped(quic, error.code(&mut self));
        }
    }

    fn try_reset<B: stream::Recv, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, Shared<'d>>,
        target: crate::stream::Target,
        code: frame::ErrorCode,
    ) -> Result<(), Reset> {
        let code = errors::Code::from_u16(code.wire_code());
        let origin = quic.streams().resident().origin();
        let id = target.stream_id();
        let mut table = quic.streams();
        if let Some(key) = target.key() {
            let local = Local::new(id, key);
            let slots = table.resident();
            let owner = slots.owner(origin).map_err(Reset::Foreign)?;
            let bound = slots.bind(owner, local).map_err(Reset::Stale)?;
            table.try_reset(bound, code, self).map_err(Reset::from)?;
            return Ok(());
        }
        let id = table.view().resolve_send(id).map_err(Reset::Stream)?;
        table
            .send()
            .reset_with(id, code, self)
            .map_err(Reset::Stream)?;
        Ok(())
    }
}

pub trait Handoff: release::Mode + Sized {
    fn defer<'req, 'd: 'req, F: rules::Policy, I: stage::Body<'req, 'd>>(
        enc: encoder::Initial<encoder::Prepared<'_, 'req, 'd, F, Infallible, I>>,
        state: response::State,
    ) -> Result<encoder::Pending<'d, Self>, conn::Error>;

    fn charge(proof: Self::Projection) -> hold::Charge;

    fn copied(copied: bool, bytes: usize) -> Option<evidence::Copy>;

    fn try_detach<'a, 'req, 'd: 'req, F: rules::Policy, I: stage::Body<'req, 'd>>(
        enc: encoder::Prepared<'a, 'req, 'd, F, Infallible, I>,
    ) -> Result<encoder::Detached<'a, 'd, F, Self, I::Deferred>, conn::Error>
    where
        I::Deferred: stage::Deferred<'d>;

    fn defer_with<'d, F: rules::Policy, D: stage::Deferred<'d>>(
        enc: encoder::Initial<encoder::Detached<'_, 'd, F, Self, D>>,
        body: D::Owner,
        outcome: life::Outcome,
    ) -> Result<encoder::Pending<'d, Self>, conn::Error>;
}

impl Handoff for Off {
    fn defer<'req, 'd: 'req, F: rules::Policy, I: stage::Body<'req, 'd>>(
        enc: encoder::Initial<encoder::Prepared<'_, 'req, 'd, F, Infallible, I>>,
        state: response::State,
    ) -> Result<encoder::Pending<'d, Self>, conn::Error> {
        enc.0.defer(state)
    }

    fn charge(proof: Self::Projection) -> hold::Charge {
        let Off = proof;
        hold::Charge::default()
    }

    fn copied(_: bool, _: usize) -> Option<evidence::Copy> {
        None
    }

    fn try_detach<'a, 'req, 'd: 'req, F: rules::Policy, I: stage::Body<'req, 'd>>(
        enc: encoder::Prepared<'a, 'req, 'd, F, Infallible, I>,
    ) -> Result<encoder::Detached<'a, 'd, F, Self, I::Deferred>, conn::Error>
    where
        I::Deferred: stage::Deferred<'d>,
    {
        Ok(enc.detach())
    }

    fn defer_with<'d, F: rules::Policy, D: stage::Deferred<'d>>(
        enc: encoder::Initial<encoder::Detached<'_, 'd, F, Self, D>>,
        body: D::Owner,
        outcome: life::Outcome,
    ) -> Result<encoder::Pending<'d, Self>, conn::Error> {
        enc.0.defer_with(body, outcome)
    }
}

impl Handoff for On {
    fn defer<'req, 'd: 'req, F: rules::Policy, I: stage::Body<'req, 'd>>(
        enc: encoder::Initial<encoder::Prepared<'_, 'req, 'd, F, Infallible, I>>,
        state: response::State,
    ) -> Result<encoder::Pending<'d, Self>, conn::Error> {
        enc.0.try_defer(state)
    }

    fn charge(proof: Self::Projection) -> hold::Charge {
        proof
    }

    fn copied(copied: bool, bytes: usize) -> Option<evidence::Copy> {
        copied.then(|| evidence::Copy::borrowed(bytes))
    }

    fn try_detach<'a, 'req, 'd: 'req, F: rules::Policy, I: stage::Body<'req, 'd>>(
        enc: encoder::Prepared<'a, 'req, 'd, F, Infallible, I>,
    ) -> Result<encoder::Detached<'a, 'd, F, Self, I::Deferred>, conn::Error>
    where
        I::Deferred: stage::Deferred<'d>,
    {
        enc.try_detach()
    }

    fn defer_with<'d, F: rules::Policy, D: stage::Deferred<'d>>(
        enc: encoder::Initial<encoder::Detached<'_, 'd, F, Self, D>>,
        body: D::Owner,
        outcome: life::Outcome,
    ) -> Result<encoder::Pending<'d, Self>, conn::Error> {
        enc.0.defer_with(body, outcome)
    }
}
