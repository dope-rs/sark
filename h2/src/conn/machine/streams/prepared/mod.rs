use core::marker;

use frame::{layout, numbers};
use modes::Mode;
use o3::{
    buffer,
    buffer::{hold, resident, segments},
};
use sark_core::{life::session, manifold::stream::Boundary};
use sark_http::{field::metadata::section::rules, payload::owned};

use crate::{
    conn,
    conn::{
        event, machine,
        machine::{ops::state, streams, streams::retained},
        outputs,
        outputs::{modes, payloads},
    },
    flow, frame, stream,
};

mod raw;

/// One source, its bounded flow debit, and the output it was admitted against.
pub(super) struct Prepared<'a, C: session::Storage, M: Mode, S> {
    stream_id: stream::Id,
    boundary: Boundary,
    output: &'a mut outputs::Outbound<C>,
    commit: Commit<'a, M>,
    source: S,
}

struct Commit<'a, M: Mode> {
    length: numbers::Length,
    next_state: stream::State,
    state: &'a mut stream::State,
    debit: flow::Debit<'a>,
    marker: marker::PhantomData<fn() -> M>,
}

struct Bound<'a, C: session::Storage, S> {
    slot: payloads::Entry<'a, C>,
    mark: payloads::Mark,
    header: [u8; frame::HEADER_LEN],
    commit: Commit<'a, modes::Retained>,
    source: S,
}

pub(super) struct Sent {
    pub(super) length: numbers::Length,
    pub(super) state: stream::State,
}

pub(super) trait Source {
    fn len(&self) -> usize;

    /// Bounds the resident DATA owner retained by this admitted prefix.
    fn resident(&self, _: usize) -> usize {
        0
    }
}

pub(super) enum Outcome<'a, C: session::Storage, M: Mode, S> {
    Ready(Prepared<'a, C, M, S>),
    Blocked(conn::DataBlock, S),
    Failed(conn::Error, S),
}

impl Source for layout::Parts<'_> {
    fn len(&self) -> usize {
        self.first.len() + self.second.len()
    }
}

impl<C: session::Storage> Source for C {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn resident(&self, amount: usize) -> usize {
        hold::Charge::from_owner(amount, buffer::Residency::resident(self)).resident_bytes
    }
}

impl Commit<'_, modes::Buffered> {
    fn commit(self) -> Sent {
        self.debit.commit();
        *self.state = self.next_state;
        Sent {
            length: self.length,
            state: self.next_state,
        }
    }
}

impl Commit<'_, modes::Retained> {
    fn commit(self) -> (Sent, flow::Reserved) {
        let credit = self.debit.reserve();
        *self.state = self.next_state;
        (
            Sent {
                length: self.length,
                state: self.next_state,
            },
            credit,
        )
    }
}

impl<'a, C: session::Storage, M: Mode, S: Source> Prepared<'a, C, M, S> {
    /// Admits flow and resident charge while retaining the source and output borrow.
    pub(super) fn prepare<R, F, A, E>(
        conn: &'a mut machine::Connection<R, F, C, A, E>,
        key: stream::Key,
        source: S,
        boundary: Boundary,
        max: streams::Limit,
    ) -> Outcome<'a, C, M, S>
    where
        R: crate::Role,
        F: rules::Policy,
        A: rules::Push,
        E: event::Evidence,
    {
        let len = source.len();
        let machine::Connection {
            egress,
            state:
                state::State {
                    settings: config,
                    flow,
                    active_streams: active,
                    ..
                },
            ..
        } = conn;
        let frame = config.peer.max_frame_size.into_usize();
        let record = match active.store.active_mut(key) {
            Some(record) => record,
            None => return Outcome::Failed(conn::Error::InactiveStream(key), source),
        };
        let send = &mut record.send_window;
        let room = match egress
            .outbound
            .room::<M>()
            .and_then(|room| room.checked_sub(frame::HEADER_LEN))
        {
            Some(room) => room,
            None => return Outcome::Blocked(conn::DataBlock::Egress, source),
        };
        if len != 0 && room == 0 {
            return Outcome::Blocked(conn::DataBlock::Egress, source);
        }
        if len != 0 && flow.send_window.available() == 0 {
            return Outcome::Blocked(conn::DataBlock::ConnectionFlow, source);
        }
        if len != 0 && send.available() == 0 {
            return Outcome::Blocked(conn::DataBlock::StreamFlow, source);
        }
        let debit = flow::Pair {
            conn: &mut flow.send_window,
            stream: send,
        }
        .debit_up_to(key, len.min(frame).min(room).min(max.get()));
        let sent = debit.len();
        let boundary = if sent == len {
            boundary
        } else {
            Boundary::More
        };
        let length = config.peer.max_frame_size.take(sent);
        let state = &mut record.state;
        let next = match state.send(stream::Event::Data { boundary }) {
            Ok(next) => next,
            Err(error) => return Outcome::Failed(conn::Error::Transition(key.id(), error), source),
        };
        if M::RETAINED
            && let Err(error) = egress.outbound.payloads.check_charge(source.resident(sent))
        {
            return Outcome::Failed(conn::Error::RetainedCharge(error), source);
        }
        Outcome::Ready(Self {
            stream_id: key.id(),
            boundary,
            output: &mut egress.outbound,
            commit: Commit {
                length,
                next_state: next,
                state,
                debit,
                marker: marker::PhantomData,
            },
            source,
        })
    }
}

impl<C: session::Storage> Prepared<'_, C, modes::Buffered, layout::Parts<'_>> {
    pub(super) fn send(self) -> Result<Sent, conn::Error> {
        // SAFETY: prepare admits the minimum of this exact source's byte sum,
        // frame size, output room, requested limit, and both flow windows. The
        // private Prepared retains these immutable slices and that same length
        // until commit, so both ranges remain within their original sources.
        let (first, second) = unsafe { raw::parts(self.source, self.commit.length.into_usize()) };
        let Self {
            stream_id: id,
            boundary,
            output,
            commit,
            source: _,
        } = self;
        let header = layout::Data::wire_header(id, boundary, commit.length);
        output.try_append([&header, first, second])?.commit();
        Ok(commit.commit())
    }
}

impl<'a, C: session::Storage, S> Prepared<'a, C, modes::Retained, S> {
    fn bind(self) -> Result<Bound<'a, C, S>, (conn::Error, S)> {
        let Self {
            stream_id: id,
            boundary,
            output,
            commit,
            source,
        } = self;
        let mark = output.head.after(output.bytes.len());
        let slot = match output.payloads.try_reserve(u64::from(id.into_u32())) {
            Ok(slot) => slot,
            Err(error) => return Err((conn::Error::Egress(error), source)),
        };
        Ok(Bound {
            slot,
            mark,
            header: layout::Data::wire_header(id, boundary, commit.length),
            commit,
            source,
        })
    }
}

impl<'a, C: session::Storage> Prepared<'a, C, modes::Retained, C> {
    pub(super) fn send(self) -> Result<(Sent, Option<C>), (conn::Error, C)> {
        let Bound {
            slot,
            mark,
            header,
            commit,
            source,
        } = self.bind()?;
        let (body, rest) = buffer::raw::Segment::try_split(source, commit.length.into_usize())
            .map_err(|(error, source)| (conn::Error::Retain(error), source))?;
        let (sent, credit) = commit.commit();
        let payload =
            payloads::Shared::data(mark, header, payloads::Inline::EMPTY, Some(body), credit);
        slot.insert(payload);
        Ok((sent, rest))
    }
}

impl<'a, 'd: 'a> Prepared<'a, resident::Shared<'d>, modes::Retained, retained::Front<'d>> {
    pub(super) fn send(
        self,
    ) -> Result<(Sent, owned::Segments<'d>), (conn::Error, segments::Nonempty<resident::Shared<'d>>)>
    {
        let Bound {
            slot,
            mark,
            header,
            commit,
            source: retained::Front(source),
        } = self
            .bind()
            .map_err(|(error, retained::Front(source))| (error, source))?;
        let (body, rest) = source
            .try_pop(commit.length.into_usize())
            .map_err(|(error, source)| (conn::Error::Retain(error), source))?;
        let (sent, credit) = commit.commit();
        let payload =
            payloads::Shared::data(mark, header, payloads::Inline::EMPTY, Some(body), credit);
        slot.insert(payload);
        Ok((sent, owned::Segments::from(rest)))
    }
}

impl<C: session::Storage> Prepared<'_, C, modes::Retained, retained::Parts<'_, C>> {
    pub(super) fn send(self) -> Result<Sent, conn::Error> {
        let Bound {
            slot,
            mark,
            header,
            commit,
            source,
        } = self.bind().map_err(|(error, _)| error)?;
        let (inline, body) = source
            .take(commit.length.into_usize())
            .map_err(conn::Error::Retain)?;
        let (sent, credit) = commit.commit();
        let payload = payloads::Shared::data(mark, header, inline, body, credit);
        slot.insert(payload);
        Ok(sent)
    }
}

const _: () = assert!(size_of::<Sent>() == 2 * size_of::<u32>());
