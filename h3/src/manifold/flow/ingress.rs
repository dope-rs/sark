use std::num::NonZero;

use conn::{scope, stream::Error};
use dope_quic::{
    conn,
    conn::stream::access::bindings,
    stream,
    wire::stream::{Direction, Id, Kind},
};
use io::ingress::schedule::Credit;
use o3::buffer::{hold, resident};
use sark_core::life::{self, mode};
use sark_http::{
    self,
    field::metadata::section::{rules, state::Selected},
    owner::{self, cause},
};

use crate::{
    conn::{event, io, io::write::Gate},
    manifold::{self, flow::report, owners},
};

/// Exclusive ingress authority borrowed from one HTTP/3 session.
#[repr(transparent)]
pub struct Ingress<'session, 'd, P: rules::Policy> {
    session: &'session mut manifold::Session<'d, P>,
}

impl<'session, 'd, P: rules::Policy> Ingress<'session, 'd, P> {
    pub(in crate::manifold) const fn new(sess: &'session mut manifold::Session<'d, P>) -> Self {
        Self { session: sess }
    }

    /// Ingests one transport event within its original QUIC scope.
    pub fn ingest<'scope, const ID: u8, B, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'scope, ID, B, RECONCILE, resident::Shared<'d>>,
        event: manifold::Notification<'scope>,
    ) -> Result<(), manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        let event = event.bind(self.session, quic)?;
        self.drive::<ID, B, (), mode::Disabled, RECONCILE>(quic, event, &mut mode::Disabled, ())?;
        self.session.egress().settle_scope(quic)?;
        Ok(())
    }

    pub(in crate::manifold) fn drive<
        'scope,
        const ID: u8,
        B,
        C: Copy,
        O: life::Mode<C, Id>,
        const RECONCILE: bool,
    >(
        &mut self,
        quic: &mut scope::Access<'scope, ID, B, RECONCILE, resident::Shared<'d>>,
        note: manifold::BoundNote<'scope>,
        life: &mut O,
        handle: C,
    ) -> Result<Option<report::Report>, manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        let manifold::BoundNote {
            event,
            owner,
            handle: bound,
        } = note;
        let key = bound.map(|handle| handle.local().key());
        let (id, result, cause) = match event {
            manifold::Event::Writable => {
                self.session.h3.session.set_gate(Gate::Open)?;
                return Ok(None);
            }
            manifold::Event::Readable { stream_id: id } => {
                if let Some(handle) = bound {
                    let mut table = quic.streams();
                    let mut binding = table.binding();
                    match binding.try_attach(handle)? {
                        bindings::Attach::Added => {
                            if id.kind() == Kind::ClientBidi
                                && self.session.h3.settings.role == event::Role::Server
                            {
                                binding.try_prepare(handle)?;
                            }
                        }
                        bindings::Attach::Present => {}
                    }
                }
                let mut net =
                    super::Transport::new(quic, &mut self.session.receive_credits, owner, bound);
                if let Some(key) = key {
                    crate::link::Pump::new(&mut net, &mut self.session.h3)
                        .token(id, key.raw().get())?;
                } else {
                    crate::link::Pump::new(&mut net, &mut self.session.h3).event(id)?;
                }
                if id.direction() == Direction::Uni
                    && let Some(handle) = bound
                    && !io::ingress::schedule::Status::new(&self.session.h3).retains(handle.local())
                {
                    quic.streams().binding().try_release(handle)?;
                }
                return self.negotiate::<ID, B, C, O, RECONCILE>(quic, life, handle);
            }
            manifold::Event::Reset {
                stream_id: id,
                error_code: code,
            } => (
                id,
                io::Receive::new(&mut self.session.h3).reset(
                    id,
                    manifold::resident_token(key)?,
                    code,
                ),
                cause::Staging::Reset,
            ),
            manifold::Event::Stopped {
                stream_id: id,
                error_code: code,
            } => (
                id,
                io::Receive::new(&mut self.session.h3).stop(
                    id,
                    manifold::resident_token(key)?,
                    code,
                ),
                cause::Staging::PeerRetired,
            ),
        };
        let charge = result.map_err(|(error, retired)| {
            Self::unstage(life, handle, id, retired, cause);
            error
        })?;
        Self::unstage(life, handle, id, charge, cause);
        if let Some(handle) = bound {
            quic.streams().binding().try_attach(handle)?;
        }
        Ok(None)
    }

    /// Consumes one event synchronously within its original QUIC scope.
    /// The callback borrows the event; its payload and held credit return afterward.
    /// Terminal bindings retire through the same resident proof as server ingress.
    pub fn consume<'scope, const ID: u8, B, const RECONCILE: bool, R>(
        &mut self,
        quic: &mut scope::Access<'scope, ID, B, RECONCILE, resident::Shared<'d>>,
        consume: impl FnOnce(&event::Event<'d, Selected<P>, P::Block>) -> R,
    ) -> Result<io::Step<R>, manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        let delivery = match self.poll(quic)? {
            io::Step::Item(delivery) => delivery,
            io::Step::More => return Ok(io::Step::More),
            io::Step::Idle => return Ok(io::Step::Idle),
        };
        let stopped = matches!(&delivery.event, event::Event::Stopped { .. });
        if let (
            event::Event::Rejected { stream_id, .. },
            manifold::Resident::Active { handle, .. } | manifold::Resident::Unbound(handle),
        ) = (&delivery.event, &delivery.resident)
        {
            self.reject(quic, *stream_id)?;
            self.session
                .h3
                .queues
                .discard(*stream_id)
                .map_err(|(error, _retired)| error)?;
            let code =
                dope_quic::errors::Code::from_u16(crate::frame::ErrorCode::Message.wire_code());
            quic.streams().try_stop(*handle, code)?;
            if matches!(stream_id.direction(), Direction::Bidi) {
                quic.streams().try_reset(
                    *handle,
                    code,
                    &mut owners::Projection::new(&mut mode::Disabled, ()),
                )?;
            }
            if !io::ingress::schedule::Status::new(&self.session.h3).rejected(*stream_id) {
                quic.streams().binding().try_release(*handle)?;
            }
        }
        let (output, consumed, resident) = {
            let manifold::Delivery { event, resident } = delivery;
            let output = consume(&event);
            let consumed = match event {
                event::Event::Data { message, data }
                | event::Event::PushData { message, data, .. } => {
                    Some((message.stream_id(), data.len()))
                }
                _ => None,
            };
            (output, consumed, resident)
        };
        if let Some((stream, bytes)) = consumed
            && Credit::new(&mut self.session.h3).is_held()
        {
            self.consumed(quic, stream, bytes)?;
        }
        let release = match resident {
            manifold::Resident::Ending(handle) => Some(handle),
            manifold::Resident::Active { handle, .. } | manifold::Resident::Unbound(handle)
                if stopped
                    && (handle.id().direction() == Direction::Uni
                        || quic.streams().try_recv_eof(handle)?)
                    && !io::ingress::schedule::Status::new(&self.session.h3)
                        .retains(handle.local()) =>
            {
                Some(handle)
            }
            _ => None,
        };
        if let Some(handle) = release {
            quic.streams().binding().try_release(handle)?;
        }
        Ok(io::Step::Item(output))
    }

    pub(in crate::manifold) fn poll<'scope, const ID: u8, B, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'scope, ID, B, RECONCILE, resident::Shared<'d>>,
    ) -> Result<io::Step<manifold::Delivery<'scope, 'd, P>>, manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        let owner = self.session.bind_scope(quic)?.owner();
        let mut net = super::Transport::new(quic, &mut self.session.receive_credits, owner, None);
        crate::link::Pump::new(&mut net, &mut self.session.h3).credit()?;
        let event = self.session.h3.events().poll_token()?;
        let event = match event {
            io::Step::Item(event) => event,
            io::Step::More => return Ok(io::Step::More),
            io::Step::Idle => return Ok(io::Step::Idle),
        };
        let detached = event.is_detached();
        let mut handle = None;
        let event = event.map_identity(|token| -> Result<_, manifold::Error> {
            if detached {
                let raw = token.raw();
                return Id::new(raw).ok_or_else(|| crate::conn::Error::Identity { raw }.into());
            }
            let key = manifold::resident_key(token)?;
            let bound = net
                .scope
                .streams()
                .resident()
                .handle_key(key)
                .ok_or(Error::InvalidStream)?;
            handle = Some(bound);
            Ok(bound.id())
        })?;
        let done = event.completion();
        let resident = match (handle, done) {
            (Some(handle), event::Completion::End(_) | event::Completion::Reset { .. }) => {
                manifold::Resident::Ending(handle)
            }
            (Some(handle), event::Completion::More | event::Completion::Stopped { .. }) => {
                match net.scope.streams().binding().try_slot(handle)? {
                    Some(slot) => manifold::Resident::Active { handle, slot },
                    None => manifold::Resident::Unbound(handle),
                }
            }
            (None, _) => manifold::Resident::None,
        };
        if let event::Event::Reset { released, .. } = &event
            && let Some(handle) = handle
        {
            self.reconcile(quic, handle.id(), *released, 0)?;
        }
        Ok(io::Step::Item(manifold::Delivery { event, resident }))
    }

    pub(in crate::manifold) fn consumed<
        const ID: u8,
        B: stream::Retain<'d>,
        const RECONCILE: bool,
    >(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        id: Id,
        bytes: usize,
    ) -> Result<(), manifold::Error> {
        self.release(quic, id, bytes)?;
        self.resume(quic, id)
    }

    pub(in crate::manifold) fn release<const ID: u8, B: stream::Recv, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        id: Id,
        bytes: usize,
    ) -> Result<(), manifold::Error> {
        let Some(bytes) = NonZero::<usize>::new(bytes) else {
            return Ok(());
        };
        let credit = self.session.receive_credits.try_take(id, bytes)?;
        if let Err(error) = Credit::new(&mut self.session.h3).release(bytes.get()) {
            return Err(manifold::Error::Budget { error, credit });
        }
        credit.release(&mut quic.streams().receive())?;
        Ok(())
    }

    pub(in crate::manifold) fn resume<
        const ID: u8,
        B: stream::Retain<'d>,
        const RECONCILE: bool,
    >(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        id: Id,
    ) -> Result<(), manifold::Error> {
        Credit::new(&mut self.session.h3).resume(id)?;
        let owner = self.session.bind_scope(quic)?.owner();
        let mut net = super::Transport::new(quic, &mut self.session.receive_credits, owner, None);
        crate::link::Pump::new(&mut net, &mut self.session.h3).credit()
    }

    pub(in crate::manifold) fn discard<const ID: u8, B: stream::Recv, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        id: Id,
    ) -> Result<(), manifold::Error> {
        self.clear(quic, id, false)
    }

    pub(in crate::manifold) fn reject<const ID: u8, B: stream::Recv, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        id: Id,
    ) -> Result<(), manifold::Error> {
        self.clear(quic, id, true)
    }

    fn clear<const ID: u8, B: stream::Recv, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        id: Id,
        retain: bool,
    ) -> Result<(), manifold::Error> {
        let credit = Credit::new(&mut self.session.h3);
        let discarded = if retain {
            let open = {
                let streams = quic.streams();
                match streams.resident().handle(id) {
                    Some(handle) if !streams.try_recv_eof(handle)? => Some(handle),
                    _ => None,
                }
            };
            let token = manifold::resident_token(open.map(|handle| handle.local().key()))?;
            let discarded = credit.reject(id, token)?;
            if let Some(handle) = open {
                quic.streams().binding().try_attach(handle)?;
            }
            discarded
        } else {
            credit.discard(id)?
        };
        let (stored, bytes) = discarded.into_parts();
        let token = match stored {
            Some(token) => Some(token),
            None => {
                let key = quic.streams().resident().key(id);
                manifold::resident_token(key)?
            }
        };
        let (queued, bytes) = match token {
            Some(token) => Credit::new(&mut self.session.h3).discard_for(token, bytes)?,
            None => (0, bytes),
        };
        self.reconcile(quic, id, bytes, queued)
    }

    fn reconcile<const ID: u8, B: stream::Recv, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        id: Id,
        bytes: usize,
        queued: usize,
    ) -> Result<(), manifold::Error> {
        let Some(credit) = self.session.receive_credits.remove(id) else {
            return if bytes == 0 && queued == 0 {
                Ok(())
            } else {
                Err(manifold::Credit::Absent {
                    stream_id: id,
                    retained: bytes,
                    queued,
                }
                .into())
            };
        };
        let Some(held) = credit
            .len()
            .checked_sub(bytes)
            .filter(|held| queued <= *held)
        else {
            return Err(manifold::Credit::Reconcile {
                stream_id: id,
                retained: bytes,
                queued,
                credit,
            }
            .into());
        };
        if let Err(error) = Credit::new(&mut self.session.h3).release(held) {
            return Err(manifold::Error::Budget { error, credit });
        }
        credit.release(&mut quic.streams().receive())?;
        Ok(())
    }

    fn negotiate<const ID: u8, B, C: Copy, O: life::Mode<C, Id>, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        life: &mut O,
        handle: C,
    ) -> Result<Option<report::Report>, manifold::Error>
    where
        B: stream::Retain<'d>,
    {
        if self.session.h3.control().has_encoder() {
            return Ok(None);
        }
        let cap = self.session.h3.qpack.qpack_encoder.maximum();
        if cap == 0 {
            return Ok(None);
        }
        let mut opened = self
            .session
            .egress()
            .settle_as::<ID, B, C, O, RECONCILE>(quic, life, handle)?;
        let bound = {
            let mut table = quic.streams();
            table.send().open_handle(Direction::Uni)
        }?;
        self.session
            .h3
            .control()
            .open_encoder(manifold::resident_target(bound))?;
        let next = self
            .session
            .egress()
            .settle_as::<ID, B, C, O, RECONCILE>(quic, life, handle)?;
        opened = opened.merged(next);
        self.session.h3.control().set_capacity(cap)?;
        Ok(Some(opened))
    }

    fn unstage<C: Copy, O: life::Mode<C, Id>>(
        life: &mut O,
        handle: C,
        id: Id,
        charge: hold::Charge,
        cause: cause::Staging,
    ) {
        if !O::ENABLED || charge.is_empty() {
            return;
        }
        owners::Projection::new(life, handle).unstage(owner::Effect::new(
            id,
            charge,
            owner::Change::StagingReleased(cause),
        ));
    }
}
