use dope::{
    core::io::transfer,
    net::link::egress::{NonEmpty, data::pair, reserve::demand},
};
use o3::{
    buffer::{bytes::window::reserve, pool, write, write::consume},
    mem::Return,
};
use sark_core::{life::session, manifold::client::io};

use crate::{
    conn::{
        outputs,
        outputs::{limits, payloads},
    },
    flow,
    frame::{controls, numbers},
    stream,
};

pub(crate) struct Transfer<'a, C: session::Storage + 'a> {
    outbound: &'a mut outputs::Outbound<C>,
}

pub(crate) struct Prepared<'a, C: session::Storage + 'a, const PREFIX: usize> {
    selected: Selected<'a, C>,
    plan: demand::Plan<C, PREFIX>,
}

pub(crate) struct Pooled<'a, C: session::Storage + 'a> {
    selected: Selected<'a, C>,
    prefix: pool::Cursor,
}

pub(crate) enum Offer<'a, C: session::Storage> {
    Prefix {
        value: NonEmpty<pool::Cursor>,
        owner: Prefix<'a, C>,
    },
    Body {
        value: NonEmpty<pair::Source<C>>,
        owner: Body<'a, C>,
    },
}

struct Pending<'a> {
    bytes: reserve::Prefix<'a, { limits::Capacity::TERMINAL_SLACK }>,
    retained: payloads::Taken<'a>,
    head: &'a mut payloads::Mark,
    barrier: &'a mut outputs::Barrier,
}

pub(crate) struct Prefix<'a, C: session::Storage> {
    pending: Pending<'a>,
    source: Option<payloads::Restorable<'a, C, payloads::Data<()>>>,
}

pub(crate) struct Body<'a, C: session::Storage> {
    pending: Pending<'a>,
    slot: payloads::Vacancy<'a, C>,
    source: Source,
}

enum Source {
    Data(payloads::Data<()>),
    Wire(payloads::Wire<()>),
}

struct Selected<'a, C: session::Storage + 'a> {
    bytes: reserve::Prefix<'a, { limits::Capacity::TERMINAL_SLACK }>,
    head: &'a mut payloads::Mark,
    barrier: &'a mut outputs::Barrier,
    bound: payloads::Front<'a, C>,
}

pub(crate) struct Tail<'a> {
    bytes: &'a mut reserve::Ring<{ limits::Capacity::TERMINAL_SLACK }>,
    retained: &'a usize,
    barrier: &'a mut outputs::Barrier,
}

impl<'a, C: session::Storage + 'a, const PREFIX: usize> Prepared<'a, C, PREFIX> {
    pub(crate) fn is_drained(&self) -> bool {
        self.selected.bytes.total() + self.selected.bound.bytes()
            == self.plan.wire_len().widen().get()
    }

    pub(crate) fn commit_to<W: write::Sink, T>(
        self,
        target: &mut W,
        commit: impl FnOnce(payloads::Write<C>, &mut Tail<'a>) -> T,
    ) -> Result<T, W::Error> {
        let (write, mut tail) = self.selected.commit_to(target)?;
        Ok(commit(write, &mut tail))
    }
}

impl<'a, C: session::Storage + 'a> Pooled<'a, C> {
    fn new(mut selected: Selected<'a, C>, mut prefix: pool::Cursor) -> Result<Self, io::Reject> {
        const {
            assert!(
                size_of::<Result<Self, io::Reject>>() == size_of::<Result<Self, pool::Cursor>>()
            );
        }
        let sources = selected.slices();
        let cause = match write::Sink::write_slices(&mut prefix, sources) {
            Ok(()) => return Ok(Self { selected, prefix }),
            Err(error) => error,
        };
        let [first, second, _, _] = sources;
        let written = prefix.write(first) + prefix.write(second);
        selected.bytes.truncate(written);
        selected.bound.defer();
        if written == 0 {
            return Err(io::Reject::Failed {
                cause: io::Error::Capacity(cause),
                prefix,
            });
        }
        Ok(Self { selected, prefix })
    }

    pub(crate) fn try_bind(self) -> Result<Offer<'a, C>, io::Failure> {
        let Selected {
            bytes,
            head,
            barrier,
            bound,
        } = self.selected;
        let (payload, retained) = bound.take();
        let pending = Pending {
            bytes,
            retained,
            head,
            barrier,
        };
        match payload {
            None => Prefix {
                pending,
                source: None,
            }
            .bind(self.prefix),
            Some((payload, slot)) => match payload {
                payloads::Shared::Data(data) => {
                    let (source, body) = data.into_parts();
                    match body {
                        Some(body) => Body {
                            pending,
                            slot,
                            source: Source::Data(source),
                        }
                        .bind(self.prefix, body),
                        None => Prefix {
                            pending,
                            source: Some((source, slot)),
                        }
                        .bind(self.prefix),
                    }
                }
                payloads::Shared::Wire(wire) => {
                    let (source, body) = wire.into_parts();
                    Body {
                        pending,
                        slot,
                        source: Source::Wire(source),
                    }
                    .bind(self.prefix, body)
                }
            },
        }
    }
}

impl<'a> Pending<'a> {
    fn commit(self) -> Tail<'a> {
        let retained = self.retained.commit();
        *self.head = self.head.after(self.bytes.len());
        Tail {
            bytes: self.bytes.commit(),
            retained,
            barrier: self.barrier,
        }
    }
}

impl<'a, C: session::Storage> Prefix<'a, C> {
    fn bind(self, prefix: pool::Cursor) -> Result<Offer<'a, C>, io::Failure> {
        match NonEmpty::try_new(prefix) {
            Ok(value) => Ok(Offer::Prefix { value, owner: self }),
            Err(cause) => {
                self.restore();
                Err(io::Failure::Prefix(cause))
            }
        }
    }

    fn restore(self) {
        if let Some((source, slot)) = self.source {
            slot.restore(payloads::Shared::Data(source.with(None)));
        }
    }

    pub(crate) fn commit(self) -> (Option<flow::Reserved>, Tail<'a>) {
        let credit = self.source.map(|(source, _)| source.credit);
        (credit, self.pending.commit())
    }
}

impl<C: session::Storage> Return for Prefix<'_, C> {
    type Item = pool::Cursor;

    fn return_item(self, _: Self::Item) {
        self.restore();
    }
}

impl<'a, C: session::Storage> Body<'a, C> {
    fn bind(self, prefix: pool::Cursor, body: C) -> Result<Offer<'a, C>, io::Failure> {
        match pair::Source::try_new(prefix, body) {
            Ok(value) => Ok(Offer::Body { value, owner: self }),
            Err(rejected) => {
                let (_, body, cause) = rejected.into_parts();
                self.restore(body);
                Err(io::Failure::Body(cause))
            }
        }
    }

    fn restore(self, body: C) {
        let payload = match self.source {
            Source::Data(source) => payloads::Shared::Data(source.with(Some(body))),
            Source::Wire(source) => payloads::Shared::Wire(source.with(body)),
        };
        self.slot.restore(payload);
    }

    pub(crate) fn commit(self) -> (Option<flow::Reserved>, Tail<'a>) {
        let Self {
            pending, source, ..
        } = self;
        let credit = match source {
            Source::Data(source) => Some(source.credit),
            Source::Wire(_) => None,
        };
        (credit, pending.commit())
    }
}

impl<C: session::Storage> Return for Body<'_, C> {
    type Item = pair::Source<C>;

    fn return_item(self, value: Self::Item) {
        self.restore(value.into_parts().1);
    }
}

impl<'a, C: session::Storage + 'a> Selected<'a, C> {
    fn slices(&self) -> [&[u8]; 4] {
        let (first, second) = self.bytes.as_slices();
        let (header, inline) = match self.bound.get() {
            Some(payload) => match payload {
                payloads::Shared::Data(data) => (data.header.as_slice(), data.inline.as_slice()),
                payloads::Shared::Wire(_) => (&[][..], &[][..]),
            },
            None => (&[][..], &[][..]),
        };
        [first, second, header, inline]
    }

    fn commit_to<W: write::Sink>(
        self,
        target: &mut W,
    ) -> Result<(payloads::Write<C>, Tail<'a>), W::Error> {
        let sources = self.slices();
        let prefix = sources.iter().map(|source| source.len()).sum();
        target.write_slices(sources)?;
        Ok(self.finish(prefix))
    }

    fn finish(self, prefix: usize) -> (payloads::Write<C>, Tail<'a>) {
        let Self {
            bytes,
            head,
            barrier,
            bound,
        } = self;
        *head = head.after(bytes.len());
        let (payload, retained) = bound.remove();
        let write = match payload {
            None => payloads::Write::Buffered {
                written: prefix,
                accepted: None,
            },
            Some(payload) => match payload {
                payloads::Shared::Wire(wire) => payloads::Write::Owned {
                    stream: wire.stream,
                    prefix_len: prefix,
                    body: wire.body,
                },
                payloads::Shared::Data(data) => match data.body {
                    Some(body) if !body.as_ref().is_empty() => payloads::Write::Split {
                        prefix_len: prefix,
                        credit: data.credit,
                        body,
                    },
                    Some(_) | None => payloads::Write::Buffered {
                        written: prefix,
                        accepted: Some(data.credit),
                    },
                },
            },
        };
        (
            write,
            Tail {
                bytes: bytes.commit(),
                retained,
                barrier,
            },
        )
    }
}

impl outputs::Reset for Tail<'_> {
    fn write(
        &mut self,
        stream: stream::Key,
        error: numbers::ErrorCode,
    ) -> Result<(), outputs::Full> {
        let limit = self.bytes.capacity();
        let wire = controls::RstStream::from_stream(stream.nonzero(), error).wire_bytes();
        consume::Error::check(*self.retained, limit).map_err(outputs::Full::Append)?;
        reserve::Append::try_new(self.bytes, [&wire], limit - *self.retained)
            .map_err(outputs::Full::Append)?
            .commit();
        Ok(())
    }

    fn set_barrier(&mut self, barrier: outputs::Barrier) {
        *self.barrier = barrier;
    }
}

impl<'a, C: session::Storage + 'a> Transfer<'a, C> {
    pub(super) fn new(outbound: &'a mut outputs::Outbound<C>) -> Self {
        Self { outbound }
    }
    pub(crate) fn drain(&mut self, bytes: usize) {
        let limit = self.outbound.front_prefix();
        let read = reserve::Prefix::new(&mut self.outbound.bytes, bytes.min(limit));
        self.outbound.head = self.outbound.head.after(read.len());
        read.commit();
    }

    pub(crate) fn bind_write(self, prefix: pool::Cursor) -> Result<Pooled<'a, C>, io::Reject> {
        if !prefix.is_empty() {
            return Err(io::Reject::Empty(prefix));
        }
        let (selected, head) = self.select(demand::Prefix::<{ payloads::MAX_PREFIX }>::FULL);
        let empty = selected
            .bound
            .get()
            .is_none_or(|payload| payload.body_len() == 0);
        if head.len().is_zero() && empty {
            return Err(io::Reject::Empty(prefix));
        }
        Pooled::new(selected, prefix)
    }

    pub(crate) fn bind_link<const PREFIX: usize>(
        self,
    ) -> Result<Option<Prepared<'a, C, PREFIX>>, io::Error> {
        use demand::Body;
        use dope::net::link::egress::load::Error;

        let (selected, prefix) = self.select(demand::Prefix::<PREFIX>::FULL);
        let body = match selected.bound.get() {
            None => None,
            Some(payload) => match payload.body() {
                Some(body) => match Body::try_new(body) {
                    Ok(body) => Some(body),
                    Err(Error::Empty(_)) => None,
                    Err(error @ (Error::Transfer(_) | Error::Resident(_))) => {
                        return Err(io::Error::Body(error));
                    }
                },
                None => None,
            },
        };
        let demand = match body {
            Some(body) => demand::Plan::with_body(prefix, body).map_err(io::Error::Overflow)?,
            None => match demand::Plan::prefix(prefix) {
                Ok(plan) => plan,
                Err(_) => return Ok(None),
            },
        };
        Ok(Some(Prepared {
            plan: demand,
            selected,
        }))
    }

    fn select<const PREFIX: usize>(
        self,
        capacity: demand::Prefix<PREFIX>,
    ) -> (Selected<'a, C>, demand::Prefix<PREFIX>) {
        let outputs::Outbound {
            bytes,
            payloads,
            head,
            terminal_barrier: barrier,
        } = self.outbound;
        let mut bytes = reserve::Prefix::new(bytes, capacity.len().into_raw());
        let mut bound = payloads.bind_front();
        let prefix = match bound.get() {
            None => capacity.min(transfer::Len::prefix(bytes.len(), transfer::Len::MAX)),
            Some(payload) => {
                let pending = payload.prefix_len(*head);
                bytes.truncate(pending);
                let leading = capacity.min(transfer::Len::prefix(bytes.len(), transfer::Len::MAX));
                if leading.len().into_raw() != pending {
                    bound.defer();
                    leading
                } else {
                    match leading.len().checked_add(payload.head_len()) {
                        Ok(prefix) if capacity.len().is_at_least(prefix) => capacity.min(prefix),
                        Ok(_) | Err(_) => {
                            bound.defer();
                            leading
                        }
                    }
                }
            }
        };
        (
            Selected {
                bytes,
                head,
                barrier,
                bound,
            },
            prefix,
        )
    }

    pub(crate) fn take_write(&mut self, write: &mut [u8]) -> Result<payloads::Write<C>, io::Error> {
        let transfer = Transfer::new(&mut *self.outbound);
        let capacity = transfer::Len::prefix(write.len(), transfer::Len::MAX);
        let capacity = demand::Prefix::<{ payloads::MAX_PREFIX }>::FULL.min(capacity);
        let (selected, _) = transfer.select(capacity);
        let mut target = write::Slice::new(write);
        let (write, _) = selected
            .commit_to(&mut target)
            .map_err(io::Error::Capacity)?;
        Ok(write)
    }
}

const _: () = {
    use core::mem::size_of;

    use o3::buffer::storage;

    type Bytes<'a> = reserve::Prefix<'a, { limits::Capacity::TERMINAL_SLACK }>;
    type Front<'a> = payloads::Front<'a, storage::Shared>;
    assert!(
        size_of::<Selected<'static, storage::Shared>>()
            == size_of::<(
                Bytes<'_>,
                &mut payloads::Mark,
                &mut outputs::Barrier,
                Front<'_>
            )>()
    );
    assert!(
        size_of::<Pending<'static>>()
            == size_of::<(
                Bytes<'_>,
                payloads::Taken<'_>,
                &mut payloads::Mark,
                &mut outputs::Barrier
            )>()
    );
};
