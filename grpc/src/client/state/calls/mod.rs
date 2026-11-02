use o3::{
    alloc,
    buffer::storage,
    depot,
    depot::{
        arena::linked::{fifo, slots},
        capacity,
    },
};
use receive::budget;
use sark_core::{
    life::session,
    manifold::{
        client::{
            Connection, domain,
            domain::{Domain as _, Plan},
        },
        schedule,
        stream::Boundary,
    },
};
use sark_h2::{conn, stream};
use sark_http::payload::response::status;
use state::output;

use crate::{
    Reason,
    client::{receive, state},
    message, metadata,
};

mod tail;

pub(in crate::client) use tail::Tail;

pub(in crate::client) enum Build<E> {
    Domain(E),
    Requests(alloc::Error),
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub(in crate::client) enum ResponseMode {
    Unary,
    Streaming,
}

/// The sole destination for a call's response lifetime.
#[derive(Clone, Copy)]
pub(in crate::client) enum Target {
    Unary(receive::UnaryKey),
    Streaming(receive::StreamKey),
}

impl Target {
    pub(in crate::client) const fn mode(self) -> ResponseMode {
        match self {
            Self::Unary(_) => ResponseMode::Unary,
            Self::Streaming(_) => ResponseMode::Streaming,
        }
    }
}

pub(in crate::client) struct Buffered<C: session::Storage = storage::Shared> {
    pub(in crate::client) frame: message::Frame<C>,
    pub(in crate::client) charge: budget::Charge,
}

pub(in crate::client) struct Response<C: session::Storage = storage::Shared> {
    pub(in crate::client) target: Target,
    pub(in crate::client) http_status: Option<status::Code>,
    pub(in crate::client) grpc: bool,
    pub(in crate::client) metadata: metadata::Decoded,
    pub(in crate::client) deframer: message::Deframer<C>,
    pub(in crate::client) message: Option<Buffered<C>>,
}

pub(in crate::client) struct Record<C: session::Storage = storage::Shared> {
    pub(in crate::client) response: Option<Response<C>>,
    pub(in crate::client) requests: Queue,
    pub(in crate::client) boundary: Boundary,
}

pub(in crate::client) struct Store<P, C = storage::Shared>
where
    C: session::Storage,
    P: Plan<C, Connection: Connection<Error = conn::Error>>,
{
    records: P::Domain<Record<C>>,
    requests: Requests,
    max_message_len: usize,
}

impl<P, C> Store<P, C>
where
    C: session::Storage,
    P: Plan<C, Connection: Connection<Error = conn::Error>>,
{
    pub(in crate::client) fn try_new(
        cap: capacity::Count,
        reqs: capacity::Count,
        max: usize,
    ) -> Result<Self, Build<P::Storage>> {
        Ok(Self {
            records: P::domain(cap).map_err(Build::Domain)?,
            requests: Requests::try_new(reqs).map_err(Build::Requests)?,
            max_message_len: max,
        })
    }

    pub(in crate::client) fn capacity(&self) -> usize {
        self.records.capacity()
    }

    pub(in crate::client) fn key_at(&self, index: usize) -> Option<P::Key> {
        self.records.key_at(index)
    }

    pub(in crate::client) fn insert(
        &mut self,
        stream: P::Key,
        target: Target,
    ) -> schedule::InsertResult<P::Key, ()> {
        use message::Deframer;
        self.records.try_insert(stream, || Record {
            response: Some(Response {
                target,
                http_status: None,
                grpc: false,
                metadata: metadata::Decoded::new(),
                deframer: Deframer::new(self.max_message_len),
                message: None,
            }),
            requests: Queue::new(),
            boundary: Boundary::More,
        })
    }

    pub(in crate::client) fn get(&self, stream: P::Key) -> Option<&Record<C>> {
        self.records.get(stream)
    }

    pub(in crate::client) fn get_mut(&mut self, stream: P::Key) -> Option<&mut Record<C>> {
        self.records.get_mut(stream)
    }

    pub(in crate::client) fn response_mut(&mut self, stream: P::Key) -> Option<&mut Response<C>> {
        self.get_mut(stream)?.response.as_mut()
    }

    /// Each unfinished response owns at most one admitted unary message charge.
    pub(in crate::client) fn charges(&self) -> impl Iterator<Item = &budget::Charge> {
        self.responses()
            .filter_map(|response| Some(&response.message.as_ref()?.charge))
    }

    pub(in crate::client) fn responses(&self) -> impl Iterator<Item = &Response<C>> {
        (0..self.records.capacity()).filter_map(|index| {
            let stream = self.records.key_at(index)?;
            self.records.get(stream)?.response.as_ref()
        })
    }

    pub(in crate::client) fn finish(&mut self, stream: P::Key) -> Option<Response<C>> {
        let (reply, clear) = {
            let record = self.get_mut(stream)?;
            let reply = record.response.take()?;
            let clear = matches!(record.boundary, Boundary::End) && record.requests.is_empty();
            (reply, clear)
        };
        if clear {
            self.records.remove(stream)?;
        }
        Some(reply)
    }

    pub(in crate::client) fn reclaim(&mut self, stream: P::Key) -> Result<(), crate::Status> {
        let clear = self.get(stream).is_some_and(|record| {
            record.response.is_none()
                && matches!(record.boundary, Boundary::End)
                && record.requests.is_empty()
        });
        if clear {
            self.records
                .remove(stream)
                .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::RequestUnknown))?;
        }
        Ok(())
    }

    pub(in crate::client) fn remove(&mut self, stream: P::Key) -> Option<Record<C>> {
        let mut record = self.records.remove(stream)?;
        self.requests.clear(&mut record.requests);
        Some(record)
    }

    pub(in crate::client) fn has_work(&self) -> bool {
        self.records.has_work()
    }

    pub(in crate::client) fn transaction<'a, Q, R, E>(
        &'a mut self,
        key: P::Key,
        check: impl FnOnce(&Record<C>, Space<'a>) -> Result<(domain::Activation<P::Block>, Q), E>,
        commit: impl FnOnce(&mut Record<C>, Q) -> R,
    ) -> Result<R, schedule::Error<E>> {
        let Self {
            records: recs,
            requests: reqs,
            ..
        } = self;
        let space = reqs.space();
        recs.transaction(key, move |record| check(record, space), commit)
    }

    pub(in crate::client) fn drive<R>(
        &mut self,
        conn: &mut P::Connection,
        egress: schedule::Egress,
        retire: &mut R,
    ) -> Result<schedule::Report, crate::Status>
    where
        P: Plan<C, Key = stream::Key>,
        R: FnMut(Record<C>),
    {
        self.records.resume(conn, egress);
        let Self {
            records: recs,
            requests: reqs,
            ..
        } = self;
        let mut driver = output::Driver::new(reqs, retire);
        recs.drive(conn, &mut driver).map_err(crate::Status::from)
    }
}

const _: () = {
    assert!(core::mem::size_of::<Option<Response>>() == core::mem::size_of::<Response>());
    assert!(
        core::mem::size_of::<Option<Buffered>>()
            <= core::mem::size_of::<Option<message::Frame>>() + 2 * core::mem::size_of::<usize>()
    );
};

/// Exclusive pending-store state held across one call transaction.
pub(in crate::client) struct Space<'a> {
    requests: &'a mut Requests,
}

/// Exclusive proof of one pending-request insertion slot.
#[repr(transparent)]
pub(in crate::client) struct Permit<'a> {
    raw: slots::Permit<'a, output::Pending, output::Tag>,
}

/// One initialized pending request not yet attached to a call queue.
#[repr(transparent)]
pub(in crate::client) struct Claim<'a> {
    raw: slots::Live<output::Pending, &'a slots::Store<output::Pending>, output::Tag>,
}

/// Pending request queue owned by one call record.
pub(in crate::client) struct Queue {
    raw: fifo::Fifo<output::Tag>,
}

/// The pending request pool tied to the call registry that owns all queues.
pub(in crate::client) struct Requests {
    raw: depot::raw::Arena<output::Pending, output::Tag>,
}

/// Exclusive access to one unchanged queue front.
pub(in crate::client) struct Front<'a> {
    raw: slots::Front<'a, output::Pending, output::Tag>,
}

impl<'a> Space<'a> {
    const fn new(requests: &'a mut Requests) -> Self {
        Self { requests }
    }

    pub(in crate::client) fn tail(self, queue: &Queue) -> Tail<'a> {
        let Some(back) = queue.raw.back() else {
            return Tail::Empty(self);
        };
        // SAFETY: Store::transaction pairs this record's live queue with its
        // Requests; Space retains exclusive access while the tail is borrowed.
        let pending = unsafe { self.requests.raw.back_mut(back) };
        Tail::Pending(pending)
    }

    pub(in crate::client) fn try_permit(self) -> Result<Permit<'a>, crate::Status> {
        let raw = self.requests.raw.permit().ok_or_else(|| {
            crate::Status::new(crate::Code::ResourceExhausted, Reason::PendingRequests)
        })?;
        Ok(Permit { raw })
    }
}

impl<'a> Permit<'a> {
    pub(in crate::client) fn claim(self, pending: output::Pending) -> Claim<'a> {
        Claim {
            raw: self.raw.claim(pending),
        }
    }
}

impl Claim<'_> {
    pub(in crate::client) fn push(self, queue: &mut Queue) {
        // SAFETY: Commit attaches this detached initialized claim to the same
        // Store record whose transaction supplied its exclusive reservation.
        slots::Live::push_back(unsafe { depot::raw::Proof::new((self.raw, &mut queue.raw)) });
    }
}

impl Queue {
    pub(in crate::client) const fn new() -> Self {
        Self {
            raw: fifo::Fifo::new(),
        }
    }

    pub(in crate::client) const fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }
}

impl Requests {
    pub(in crate::client) fn try_new(capacity: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self {
            raw: depot::raw::Arena::try_new(capacity)?,
        })
    }

    pub(in crate::client) fn space(&mut self) -> Space<'_> {
        Space::new(self)
    }

    pub(in crate::client) fn front<'a>(&'a self, queue: &'a mut Queue) -> Option<Front<'a>> {
        Some(Front {
            // SAFETY: Store::drive pairs its Requests with one of its own
            // records; the exclusive queue borrow retains that live partition.
            raw: unsafe { self.raw.queue(&mut queue.raw) }.front_entry()?,
        })
    }

    pub(in crate::client) fn clear(&self, queue: &mut Queue) {
        // SAFETY: Store removal and Driver settlement pass their own record's
        // queue, whose live nodes belong exclusively to this Requests arena.
        let mut queue = unsafe { self.raw.queue(&mut queue.raw) };
        while queue.pop_front().is_some() {}
    }
}

impl Front<'_> {
    pub(in crate::client) fn get_mut(&mut self) -> &mut output::Pending {
        self.raw.get_mut()
    }

    pub(in crate::client) fn discard(self) {
        self.raw.discard();
    }
}

const _: () = {
    assert!(core::mem::size_of::<Permit<'static>>() == core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Claim<'static>>() == 2 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Space<'static>>() == core::mem::size_of::<usize>());
};
