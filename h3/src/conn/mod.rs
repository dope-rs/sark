use std::{marker, mem};

use dope_quic::{
    conn::stream::resident::Origin,
    wire::{
        stream::{Direction, Id},
        varint,
    },
};
use o3::{
    buffer::{hold, resident, segments, storage::inline},
    depot::{array::Index, map},
};
use sark_core::manifold::stream::Boundary;
use sark_http::{
    field,
    field::metadata::section::{rules, state::Selected},
};

use self::{
    io::{ingress::stream::table, outbound, replies, write},
    options::plan,
};
use crate::{
    frame,
    qpack::{decoder, decoder::state::State, encoder},
    stream,
};

mod blocked;
mod discard;
mod error;
pub mod event;
pub mod io;
pub mod options;
pub mod output;
pub mod push;
pub use discard::Discard;
pub use error::Error;

pub const INLINE_PREFIX_CAPACITY: usize = inline::CAPACITY;

pub struct Settings {
    pub(super) role: event::Role,
    pub(super) local_settings: frame::Settings,
    pub(super) peer_settings: Option<frame::Settings>,
    pub(super) max_frame_size: usize,
}

impl Settings {
    pub fn peer(&self) -> Option<&frame::Settings> {
        self.peer_settings.as_ref()
    }

    pub fn local(&self) -> &frame::Settings {
        &self.local_settings
    }
}

pub(super) struct QpackState {
    pub(super) qpack_encoder: encoder::Encoder,
    pub(super) qpack_decoder: decoder::Decoder,
    pub(super) feedback: State,
    blocked: blocked::Blocked,
}

pub(super) struct SessionState {
    origin: Binding,
    pub(super) control: Option<stream::Target>,
    pub(super) qpack_encoder: Option<stream::Target>,
    pub(super) qpack_decoder: Option<stream::Target>,
    pub(super) goaway_id: Option<u64>,
    pub(super) sent_goaway_id: Option<u64>,
    pub(super) max_push_id: Option<u64>,
    pub(in crate::conn) pushes: push::History,
}

impl SessionState {
    fn new(plan: map::Plan, limit: usize) -> Self {
        Self {
            origin: Binding::default(),
            control: None,
            qpack_encoder: None,
            qpack_decoder: None,
            goaway_id: None,
            sent_goaway_id: None,
            max_push_id: None,
            pushes: push::History::new(plan, limit),
        }
    }

    fn check_cancel(&self, id: u64, role: event::Role) -> Result<(), Error> {
        push::check(id, self.max_push_id).map_err(Error::Push)?;
        if role == event::Role::Server && !self.pushes.contains(id) {
            return Err(Error::Push(push::Error::Unknown { push_id: id }));
        }
        Ok(())
    }
    fn is_critical(&self, id: Id) -> bool {
        [self.control, self.qpack_encoder, self.qpack_decoder]
            .into_iter()
            .flatten()
            .any(|target| target.stream_id() == id)
    }

    pub(super) fn write_gate(&self) -> write::Gate {
        self.origin.gate()
    }

    pub(super) fn set_gate(&mut self, gate: write::Gate) -> Result<(), Error> {
        self.origin.set_gate(gate)
    }
}

const _: () = {
    assert!(mem::size_of::<SessionState>() == 22 * mem::size_of::<u64>());
    assert!(mem::align_of::<SessionState>() == mem::align_of::<u64>());
};

#[derive(Clone, Copy, Default)]
enum Binding {
    #[default]
    Unbound,
    Open(Origin),
    Held(Origin),
}

impl Binding {
    const fn origin(self) -> Option<Origin> {
        match self {
            Self::Unbound => None,
            Self::Open(origin) | Self::Held(origin) => Some(origin),
        }
    }

    const fn gate(self) -> write::Gate {
        match self {
            Self::Unbound | Self::Open(_) => write::Gate::Open,
            Self::Held(_) => write::Gate::Held,
        }
    }

    fn bind(&mut self, origin: Origin) -> Result<(), Error> {
        let existing = match *self {
            Self::Unbound => {
                *self = Self::Open(origin);
                return Ok(());
            }
            Self::Open(existing) | Self::Held(existing) => existing,
        };
        Err(Error::Bound {
            existing: existing.raw(),
            attempted: origin.raw(),
        })
    }

    fn set_gate(&mut self, gate: write::Gate) -> Result<(), Error> {
        let origin = match *self {
            Self::Unbound => {
                return Err(match gate {
                    write::Gate::Open => Error::UnboundOpen,
                    write::Gate::Held => Error::UnboundHold,
                });
            }
            Self::Open(origin) | Self::Held(origin) => origin,
        };
        *self = match gate {
            write::Gate::Open => Self::Open(origin),
            write::Gate::Held => Self::Held(origin),
        };
        Ok(())
    }
}

const _: () = assert!(mem::size_of::<Binding>() == 2 * mem::size_of::<u64>());

pub(super) struct ByteBudget {
    retained: usize,
    limit: usize,
}

impl ByteBudget {
    pub(super) const fn new(limit: usize) -> Self {
        Self { retained: 0, limit }
    }

    pub(in crate::conn) fn try_reserve(&mut self, bytes: usize) -> Result<(), Error> {
        let retained = self
            .retained
            .checked_add(bytes)
            .filter(|retained| *retained <= self.limit)
            .ok_or(Error::ReceiveCapacity {
                retained: self.retained,
                additional: bytes,
                limit: self.limit,
            })?;
        self.retained = retained;
        Ok(())
    }

    pub(in crate::conn) fn release(&mut self, bytes: usize) -> Result<(), Error> {
        let Some(retained) = self.retained.checked_sub(bytes) else {
            return Err(Error::ReceiveRelease {
                retained: self.retained,
                released: bytes,
            });
        };
        self.retained = retained;
        Ok(())
    }
}

fn try_add_credit(accumulated: usize, additional: usize) -> Result<usize, Error> {
    accumulated
        .checked_add(additional)
        .ok_or(Error::CreditOverflow {
            accumulated,
            additional,
        })
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CreditMode {
    Open,
    Held,
}

pub(super) struct IngressState<'d> {
    pub(super) budget: ByteBudget,
    pub(super) segments: segments::Pool<resident::Shared<'d>>,
    pub(super) data_credit: CreditMode,
}

pub struct Connection<'d, P: rules::Policy = rules::RetainRaw, A: rules::Push = rules::RejectPush> {
    pub(super) settings: Settings,
    pub(super) qpack: QpackState,
    pub(super) session: SessionState,
    streams: table::Table<'d>,
    pub(super) queues: write::Queues<'d, Selected<P>, P::Block>,
    pub(super) ingress: IngressState<'d>,
    pub(super) validation: marker::PhantomData<fn() -> (P, A)>,
}

impl<'d> Connection<'d> {
    pub fn with_role(role: event::Role) -> Result<Self, options::Error> {
        Self::with_config(role, options::Config::default())
    }

    pub fn with_config(role: event::Role, config: options::Config) -> Result<Self, options::Error> {
        plan::Config::new(config)?.build(role)
    }

    pub fn with_push<A: rules::Push>(
        role: event::Role,
    ) -> Result<Connection<'d, rules::RetainRaw, A>, options::Error> {
        plan::Config::new(options::Config::default())?.build::<rules::RetainRaw, A>(role)
    }
}

impl<'d, P: rules::Policy, A: rules::Push> Connection<'d, P, A> {
    pub fn control(&mut self) -> io::Control<'_, 'd, P, A> {
        io::Control::new(self)
    }

    pub fn reply(&mut self) -> replies::Reply<'_, 'd, P, A> {
        replies::Reply::new(self)
    }

    pub fn events(&mut self) -> io::Events<'_, 'd, P, A> {
        io::Events::new(self)
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    pub(in crate::conn) fn stage<'a, S: field::Source>(
        &'a mut self,
        target: stream::Target,
        kind: varint::VarInt,
        leading: Option<varint::VarInt>,
        source: S,
        boundary: Boundary,
    ) -> Result<outbound::Pending<'a, 'd, Selected<P>, P::Block>, Error> {
        let qpack = self.session.qpack_encoder;
        let (fields, method) = outbound::Section::frame(
            target,
            kind,
            leading,
            &mut self.qpack.qpack_encoder,
            self.settings.peer_settings.as_ref(),
            source,
            boundary,
        )?;
        let method = if self.settings.role == event::Role::Client {
            method
        } else {
            None
        };
        let outgoing = match self
            .streams
            .outgoing(target, &self.ingress.segments, method)
        {
            Ok(outgoing) => outgoing,
            Err(error) => {
                fields.discard();
                return Err(error);
            }
        };
        Ok(outbound::Pending::new(
            fields,
            &mut self.queues,
            qpack,
            outgoing,
        ))
    }

    pub(crate) fn origin(&self) -> Option<Origin> {
        self.session.origin.origin()
    }

    pub(crate) fn bind(&mut self, origin: Origin) -> Result<(), Error> {
        self.session.origin.bind(origin)
    }

    pub(crate) fn admit(&self) -> Result<(), Error> {
        match self.session.goaway_id {
            Some(cutoff) => Err(Error::GoAway(cutoff)),
            None => Ok(()),
        }
    }

    pub fn wire_len(&self) -> usize {
        self.queues.write_budget.wire_len()
    }

    pub fn resident_len(&self) -> usize {
        self.queues.write_budget.resident_len()
    }

    pub fn retained(&self, id: Id) -> Result<hold::Charge, Error> {
        let mut retained = Ok(hold::Charge::default());
        for lane in [Index::at::<0>(), Index::at::<1>()] {
            self.queues.writes.visit_owner(id.get(), lane, |write| {
                if let Ok(total) = retained {
                    retained = total.try_add(write.retained());
                }
            });
        }
        retained.map_err(Error::Retention)
    }

    pub(super) fn release_rx(&mut self, bytes: usize) -> Result<(), Error> {
        self.ingress.budget.release(bytes)
    }
}

const _: () = assert!(mem::size_of::<ByteBudget>() == 2 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Option<Origin>>() == mem::size_of::<u64>());

fn start_uni_stream<P: rules::Policy, A: rules::Push>(
    state: &mut Connection<'_, P, A>,
    target: stream::Target,
    stream: frame::Stream<'_>,
) -> Result<(), Error> {
    use write::budget::WriteClass;

    let id = target.stream_id();
    if matches!(id.direction(), Direction::Bidi) {
        return Err(Error::StreamKind {
            stream_id: id.get(),
        });
    }
    let prefix = output::WritePrefix::try_encode(&stream)?;
    state.queues.enqueue(write::Entry::single(
        WriteClass::Critical,
        target,
        prefix,
        Boundary::More,
    ))
}

fn frame_prefix(kind: varint::VarInt, length: varint::VarInt) -> output::WritePrefix {
    let kind = kind.pack();
    let length = length.pack();
    let mut prefix = inline::Bytes::new();
    let fill = inline::Fill::new(&mut prefix);
    let (fill, _) = fill.write::<8, 16>((&kind).into());
    let (_, _) = fill.write::<8, 8>((&length).into());
    output::WritePrefix::Inline(prefix)
}
