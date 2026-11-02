use core::task::Poll;

use o3::{
    buffer,
    buffer::storage,
    depot::{arena::linked::slots, capacity, queue::pool},
};
use sark_core::{manifold, manifold::schedule};

use crate::{
    Reason, details, message, metadata,
    server::{
        call::credits,
        egress::{self, Payload, pending},
        service::{
            handler::Production,
            reply::{Write, proof::Proof},
        },
    },
};

enum Source<'a, const N: usize> {
    Retained(storage::Shared),
    Vector(Vec<u8>),
    Slices(Option<buffer::raw::Slices<'a, N>>),
}

impl<const N: usize> Source<'_, N> {
    fn len(&self) -> usize {
        match self {
            Self::Retained(payload) => payload.len(),
            Self::Vector(payload) => payload.len(),
            Self::Slices(source) => source
                .as_ref()
                .map_or(0, |source| source.len().get() as usize),
        }
    }

    fn try_build(self) -> Result<storage::Shared, crate::Status> {
        match self {
            Self::Retained(payload) => Ok(payload),
            Self::Vector(mut payload) => {
                Ok(storage::Shared::try_from(&mut payload)
                    .map_err(storage::AllocError::Exhausted)?)
            }
            Self::Slices(Some(source)) => Ok(source
                .try_owned()
                .map_err(storage::AllocError::Exhausted)?
                .into_shared()),
            Self::Slices(None) => Ok(storage::Shared::new()),
        }
    }
}

struct Message<'a> {
    payload: Payload,
    slot: slots::Vacant<Payload, slots::Bound<'a, Payload>>,
    credit: credits::Update<'a>,
}

enum Output<'a> {
    Exhausted,
    Ready {
        staged: credits::Staged<'a, Payload>,
        slot: slots::Vacant<Payload, slots::Bound<'a, Payload>>,
    },
}

enum Position<'a, K: manifold::raw::Key> {
    Vacant(&'a mut Option<pending::Pending<K>>),
    Open(&'a mut pending::Pending<K>),
    Complete(&'a mut pending::Pending<K>),
}

pub(in crate::server) struct Draft<'a, K: manifold::raw::Key> {
    key: K,
    position: Position<'a, K>,
    output: Output<'a>,
    active: &'a mut usize,
    message_limit: capacity::Count,
    wire_limit: usize,
    ready: bool,
}

pub(super) struct Prepared<'a, K: manifold::raw::Key> {
    key: K,
    change: Change<'a, K>,
    message: Option<Message<'a>>,
    life: Lifecycle,
    work: Option<schedule::WorkClass>,
}

enum Change<'a, K: manifold::raw::Key> {
    None,
    Open {
        slot: &'a mut Option<pending::Pending<K>>,
        pending: pending::Pending<K>,
        active: &'a mut usize,
        total: usize,
    },
    Trailer {
        pending: &'a mut pending::Pending<K>,
        trailer: pending::Trailer,
    },
}

#[repr(u8)]
pub(super) enum Lifecycle {
    Keep,
    Open,
}

pub(in crate::server) enum PushError {
    Full,
    Credit(details::Credit),
    Status(crate::Status),
}

impl<'a, K: manifold::raw::Key> Draft<'a, K> {
    pub(in crate::server) fn try_new(
        store: &'a mut egress::Store<K>,
        key: K,
    ) -> Result<Self, crate::Status> {
        let lane = store.lanes.try_bind_mut(key.slot())?;
        let ready = !lane.is_empty();
        let (slot, state) = lane.reserve();
        let position = match &mut state.pending {
            Some(pending) => {
                if pending.key != key {
                    return Err(Self::invalid(Reason::StaleStream));
                }
                if pending.trailer.is_some() {
                    Position::Complete(pending)
                } else {
                    Position::Open(pending)
                }
            }
            vacant @ None if !ready => Position::Vacant(vacant),
            None => return Err(Self::invalid(Reason::IngressAccounting)),
        };
        Ok(Self {
            key,
            position,
            output: match slot {
                pool::Vacancy::Exhausted => Output::Exhausted,
                pool::Vacancy::Ready(slot) => Output::Ready {
                    slot,
                    staged: credits::Staged::new(&mut store.credits, &mut state.charge),
                },
            },
            active: &mut store.active,
            message_limit: store.message_limit,
            wire_limit: store.wire_limit,
            ready,
        })
    }

    fn pending_full() -> crate::Status {
        crate::Status::new(
            crate::Code::ResourceExhausted,
            Reason::PendingResponseCapacity,
        )
    }

    fn invalid(reason: Reason) -> crate::Status {
        crate::Status::new(crate::Code::Internal, reason)
    }

    fn framed_len(
        position: &Position<'_, K>,
        limit: capacity::Count,
        len: usize,
    ) -> Result<usize, PushError> {
        let limit = limit.get();
        if len > limit {
            return Err(PushError::Status(crate::Status::from(
                message::Error::MessageTooLarge { len, max: limit },
            )));
        }
        if matches!(position, Position::Complete(_)) {
            return Err(PushError::Status(Self::invalid(Reason::OutputComplete)));
        }
        Ok(len + message::HEADER_LEN)
    }

    pub(in crate::server) fn push(&mut self, payload: storage::Shared) -> Result<(), PushError> {
        self.push_with(Source::<0>::Retained(payload))
    }

    pub(in crate::server) fn push_encoded<C: crate::Codec>(
        &mut self,
        codec: &mut C,
        item: &C::Encode,
    ) -> Result<(), PushError> {
        let empty = self.is_empty();
        codec.encode(item, &mut *self).map_err(|status| {
            if empty {
                self.clear();
            }
            PushError::Status(status)
        })?
    }

    fn push_with<const N: usize>(&mut self, source: Source<'_, N>) -> Result<(), PushError> {
        let Self {
            output,
            position,
            message_limit: limit,
            wire_limit: wire,
            ..
        } = self;
        let staged = match output {
            Output::Exhausted => None,
            Output::Ready { staged, .. } => {
                if !staged.is_empty() {
                    return Err(PushError::Status(Self::invalid(Reason::ResponsePerTurn)));
                }
                Some(staged)
            }
        };
        let bytes = Self::framed_len(position, *limit, source.len())?;
        if bytes > *wire {
            return Err(PushError::Status(crate::Status::credit(
                Reason::PendingResponseCapacity,
                details::Credit::Limit {
                    account: details::Account::Charge,
                    required: bytes,
                    capacity: *wire,
                },
            )));
        }
        let admit = staged
            .ok_or(PushError::Full)?
            .try_grow(bytes, *wire, *wire)
            .map_err(PushError::Credit)?;
        let payload = source.try_build().map_err(PushError::Status)?;
        admit.write(Payload(payload));
        Ok(())
    }

    pub(super) fn prepare(
        self,
        metadata: metadata::Metadata,
        trailers: metadata::Metadata,
        status: Option<crate::Status>,
        life: Lifecycle,
    ) -> Result<Prepared<'a, K>, crate::Status> {
        let Self {
            key,
            position,
            output,
            active,
            ready,
            ..
        } = self;
        if status.is_none() && !trailers.is_empty() {
            return Err(Self::invalid(Reason::TrailingMetadata));
        }
        let trailer = status
            .map(|status| pending::Trailer::new(status, trailers))
            .transpose()?;
        let message = match output {
            Output::Exhausted => None,
            Output::Ready { slot, staged } => staged.finish().map(|(payload, credit)| Message {
                payload,
                slot,
                credit,
            }),
        };
        let altered = message.is_some()
            || trailer.is_some()
            || matches!(life, Lifecycle::Open)
            || !metadata.is_empty();
        let (change, work) = match position {
            Position::Vacant(slot) if altered => {
                let total = active.checked_add(1).ok_or_else(Self::pending_full)?;
                let mut pending = pending::Pending::new(key, metadata);
                pending.trailer = trailer;
                (
                    Change::Open {
                        slot,
                        pending,
                        active,
                        total,
                    },
                    Some(schedule::WorkClass::Control),
                )
            }
            Position::Vacant(_) => (Change::None, None),
            Position::Complete(_) if altered => return Err(Self::invalid(Reason::OutputComplete)),
            Position::Complete(pending) => (Change::None, pending.work(ready)),
            Position::Open(pending) => {
                if !metadata.is_empty() {
                    return Err(Self::invalid(Reason::MetadataCommitted));
                }
                let work = pending.work(ready).or_else(|| {
                    if message.is_some() {
                        Some(schedule::WorkClass::Data)
                    } else if trailer.is_some() {
                        Some(schedule::WorkClass::Control)
                    } else {
                        None
                    }
                });
                let change = match trailer {
                    Some(trailer) => Change::Trailer { pending, trailer },
                    None => Change::None,
                };
                (change, work)
            }
        };
        Ok(Prepared {
            key,
            change,
            message,
            life,
            work,
        })
    }
}

impl<K: manifold::raw::Key> crate::encode::Sink for &mut Draft<'_, K> {
    type Output = Result<(), PushError>;

    fn try_write<const N: usize>(
        self,
        source: Option<buffer::raw::Slices<'_, N>>,
    ) -> Result<Self::Output, crate::Status> {
        Ok(self.push_with(Source::Slices(source)))
    }
}

impl<K: manifold::raw::Key> Prepared<'_, K> {
    pub(super) fn key(&self) -> K {
        self.key
    }
    pub(super) fn work(&self) -> Option<schedule::WorkClass> {
        self.work
    }
    pub(super) fn apply(self, output: &mut Production) {
        if matches!(self.life, Lifecycle::Open) {
            *output = Production::Ready;
        }
        match self.change {
            Change::None => {}
            Change::Open {
                slot,
                pending,
                active,
                total,
            } => {
                *slot = Some(pending);
                *active = total;
            }
            Change::Trailer { pending, trailer } => pending.trailer = Some(trailer),
        }
        if let Some(Message {
            payload,
            slot,
            credit,
        }) = self.message
        {
            slot.append(payload);
            credit.commit();
        }
    }
}

impl<K: manifold::raw::Key> Proof for Draft<'_, K> {}

impl<'a, K: manifold::raw::Key> Write for Draft<'a, K> {
    fn push_message(&mut self, payload: Vec<u8>) -> Result<(), crate::Status> {
        self.push_with(Source::<0>::Vector(payload))
            .map_err(PushError::into_status)
    }

    fn push(&mut self, payload: storage::Shared) -> Result<(), crate::Status> {
        Draft::push(self, payload).map_err(PushError::into_status)
    }
    fn encode<C: crate::Codec>(
        &mut self,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<(), crate::Status> {
        Draft::push_encoded(self, codec, message).map_err(PushError::into_status)
    }
    fn try_encode<C: crate::Codec>(
        &mut self,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<Poll<()>, crate::Status> {
        match Draft::push_encoded(self, codec, message) {
            Ok(()) => Ok(Poll::Ready(())),
            Err(PushError::Full | PushError::Credit(details::Credit::Limit { .. })) => {
                Ok(Poll::Pending)
            }
            Err(error) => Err(error.into_status()),
        }
    }
    fn clear(&mut self) {
        if let Output::Ready { staged, .. } = &mut self.output {
            staged.clear();
        }
    }
    fn len(&self) -> usize {
        usize::from(matches!(&self.output, Output::Ready { staged, .. } if !staged.is_empty()))
    }
    fn poll_ready(&self) -> Result<Poll<()>, crate::Status> {
        if !self.is_empty() {
            return Err(Self::invalid(Reason::ResponsePerTurn));
        }
        Self::framed_len(&self.position, self.message_limit, 0).map_err(PushError::into_status)?;
        match &self.output {
            Output::Exhausted => Ok(Poll::Pending),
            Output::Ready { staged, .. } => {
                match staged.check(message::HEADER_LEN, self.wire_limit, self.wire_limit) {
                    Ok(_) => Ok(Poll::Ready(())),
                    Err(details::Credit::Limit { .. }) => Ok(Poll::Pending),
                    Err(error) => Err(PushError::Credit(error).into_status()),
                }
            }
        }
    }
}

impl PushError {
    fn into_status(self) -> crate::Status {
        match self {
            Self::Full => crate::Status::new(
                crate::Code::ResourceExhausted,
                Reason::PendingResponseCapacity,
            ),
            Self::Credit(error) => crate::Status::credit(Reason::PendingResponseCapacity, error),
            Self::Status(status) => status,
        }
    }
}
