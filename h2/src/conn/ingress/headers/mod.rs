use std::{iter, ops};

use active::{Active, Source};
use decode::mode::Discard;
use o3::buffer::{pool, write::consume::Consume as _};
use sark_core::{
    life::session,
    manifold::stream::{Boundary, Section},
};
use sark_http::{
    self,
    field::metadata::{
        block,
        section::{rules, state::Selected, storage},
    },
    payload::compress::dynamic::{mode, plan},
};
use sink::output;
use state::{Mode, State};

use crate::{
    conn,
    conn::{
        build, event,
        ingress::{
            Decode, events, header,
            header::sink,
            input::{self, Source as _},
        },
        registry,
    },
    frame::numbers,
    hpack::{decode, decode::phase::Phase},
    stream,
};

mod active;
mod fragment;
mod state;

pub(crate) use fragment::Fragment;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    Deliver,
    Reset(numbers::ErrorCode),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Headers {
        boundary: Boundary,
        section: Section,
        action: Action,
    },
    PushPromise {
        promised: stream::Nonzero,
        associated: registry::Association,
    },
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Continuation(u32);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pending {
    pub(crate) stream_id: stream::Nonzero,
    pub(crate) kind: Kind,
    pub(crate) continuations: Continuation,
}

pub(crate) struct Blocks<P: rules::Policy> {
    header_pool: pool::Growing,
    header_slots: usize,
    decoder: State<P>,
    header_cap: usize,
    connect: block::Connect,
}

impl Continuation {
    const LIMIT: u32 = 64;
    pub(crate) const ZERO: Self = Self(0);

    pub(crate) fn next(self) -> Result<Self, conn::Error> {
        let actual = self.0 + 1;
        if actual > Self::LIMIT {
            return Err(conn::Error::Continuation(conn::Continuation::Limit {
                actual,
                limit: Self::LIMIT,
            }));
        }
        Ok(Self(actual))
    }
}

impl<P: rules::Policy> Blocks<P> {
    pub(crate) fn acknowledge(&mut self, maximum: usize) -> Result<(), conn::Error> {
        self.decoder.map_idle(|mut decoder, scratch| {
            decoder.set_max_size(maximum);
            (Mode::Idle(decoder, scratch), Ok(()))
        })
    }

    pub(crate) fn try_new(
        plan: pool::Layout,
        table: plan::Plan<mode::Staged>,
        cap: usize,
        connect: block::Connect,
    ) -> Result<Self, build::Ingress> {
        let plan = storage::try_layout::<P::Block>(plan, cap)
            .map_err(|(_, error)| build::Ingress::HeaderLayout(error))?;
        let mut dec = decode::Decoder::try_from(table).map_err(build::Ingress::Decoder)?;
        dec.set_max_list(Some(cap));
        Ok(Self {
            header_pool: pool::Growing::try_new(plan).map_err(build::Ingress::Pool)?,
            header_slots: plan.slots() as usize,
            decoder: State::new(dec),
            header_cap: cap,
            connect,
        })
    }

    fn admit(used: usize, incoming: usize, limit: usize) -> Result<usize, conn::Error> {
        if used > limit || incoming > limit - used {
            return Err(conn::Error::HeaderList {
                used,
                incoming,
                limit,
            });
        }
        Ok(used + incoming)
    }

    pub(crate) fn begin<C: session::Storage, A: rules::Push>(
        &mut self,
        kind: Option<rules::Kind>,
        head: Pending,
        input: &mut header::Input<'_, '_, C>,
        part: Fragment,
    ) -> Result<(), conn::Error> {
        self.start_mode(kind, head)?;
        let bytes = input.get(part.range())?;
        self.feed_parts::<A>(bytes.len(), bytes.fragments())?;
        input.consume(part.total)
    }

    fn start_mode(&mut self, kind: Option<rules::Kind>, head: Pending) -> Result<(), conn::Error> {
        let pool = &mut self.header_pool;
        let slots = self.header_slots;
        let connect = self.connect;
        self.decoder.map_idle(|decoder, scratch| match kind {
            Some(kind) => {
                let lease = match pool.try_acquire() {
                    Ok(Some(lease)) => lease,
                    Ok(None) => {
                        return (
                            Mode::Idle(decoder, scratch),
                            Err(conn::Error::HeaderDecodeFull {
                                used: slots,
                                limit: slots,
                            }),
                        );
                    }
                    Err(cause) => {
                        return (Mode::Idle(decoder, scratch), Err(cause.into()));
                    }
                };
                let sink = match sink::Header::try_new(lease, kind, connect, scratch) {
                    Ok(sink) => sink,
                    Err((scratch, cause)) => {
                        return (
                            Mode::Idle(decoder, scratch),
                            Err(conn::Error::Header(cause)),
                        );
                    }
                };
                let budget = decoder.budget();
                let block = Phase::<Decode>::Representation(decoder);
                (
                    Mode::Active(Active {
                        pending: head,
                        source: Source::Store {
                            block,
                            budget,
                            sink,
                        },
                        wire_len: 0,
                    }),
                    Ok(()),
                )
            }
            None => {
                let budget = decoder.budget();
                (
                    Mode::Active(Active {
                        pending: head,
                        source: Source::Discard(
                            Phase::<Discard>::Representation(decoder),
                            budget,
                            scratch,
                        ),
                        wire_len: 0,
                    }),
                    Ok(()),
                )
            }
        })
    }

    pub(crate) fn discard<C: session::Storage>(
        &mut self,
        input: &mut header::Input<'_, '_, C>,
        part: Fragment,
    ) -> Result<(), conn::Error> {
        let total = part.total;
        let bytes = input.get(part.range())?;
        Self::admit(0, bytes.len(), self.header_cap)?;
        self.decoder.map_idle(|decoder, scratch| {
            let (first, rest) = bytes.split();
            let decoded = match rest {
                None => decoder.discard(first),
                Some(rest) => {
                    let mut budget = decoder.budget();
                    let block = Phase::<Discard>::Representation(decoder);
                    match block.feed_all(&mut budget, iter::once(first).chain(rest), ()) {
                        Ok((block, ())) => block.finish(),
                        Err(((), cause)) => Err(cause),
                    }
                }
            };
            match decoded {
                Ok(decoder) => (Mode::Idle(decoder, scratch), Ok(())),
                Err(cause) => (Mode::Failed, Err(conn::Error::Hpack(cause))),
            }
        })?;
        input.consume(total)
    }

    pub(crate) fn prepare<'a, C: session::Storage, A: rules::Push, E: event::Evidence>(
        &mut self,
        input: &'a mut header::Input<'_, '_, C>,
        events: &'a mut events::Events<Selected<P>, P::Block, C, E>,
        part: Fragment,
    ) -> Result<header::Prepared<'a, P, C, E>, conn::Error> {
        let total = part.total;
        let event = events.vacant()?;
        let out = self.decode_all::<C, A>(input, part)?;
        let frame = input.prepare(total)?;
        Ok(header::Prepared::new(
            frame,
            event,
            out.head,
            out.validation,
        ))
    }

    pub(crate) fn feed_range<C: session::Storage, A: rules::Push>(
        &mut self,
        input: &input::FrameInput<C>,
        range: ops::Range<usize>,
    ) -> Result<(), conn::Error> {
        let len = range.len();
        let offset = range.start;
        let bytes = input.bytes.get(range).ok_or(conn::Error::InputRange {
            offset,
            need: len,
            have: input.bytes.len(),
        })?;
        self.feed_parts::<A>(len, bytes.fragments())
    }

    fn feed_parts<'a, A: rules::Push>(
        &mut self,
        len: usize,
        parts: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<(), conn::Error> {
        let limit = self.header_cap;
        self.decoder.map_active(|active| {
            let wire_len = match Self::admit(active.wire_len, len, limit) {
                Ok(wire_len) => wire_len,
                Err(error) => return (Mode::Active(active), Err(error)),
            };
            match active.decode::<A, _>(parts, wire_len) {
                Ok(active) => (Mode::Active(active), Ok(())),
                Err(cause) => (Mode::Failed, Err(cause.into())),
            }
        })
    }

    /// Feeds one complete non-final CONTINUATION payload directly from its
    /// receive owner. Such a fragment cannot publish an event, so it needs no
    /// retained-input node once HPACK has consumed it synchronously.
    pub(crate) fn feed_direct<A: rules::Push>(&mut self, part: &[u8]) -> Result<(), conn::Error> {
        self.feed_parts::<A>(part.len(), [part])
    }

    pub(crate) fn resume<'a, C: session::Storage, A: rules::Push, E: event::Evidence>(
        &mut self,
        input: &'a mut input::FrameInput<C>,
        events: &'a mut events::Events<Selected<P>, P::Block, C, E>,
        range: ops::Range<usize>,
        total: usize,
    ) -> Result<header::Prepared<'a, P, C, E>, conn::Error> {
        let event = events.vacant()?;
        self.feed_range::<C, A>(input, range)?;
        let out = self.decoder.map_active(|active| match active.source {
            Source::Store {
                block,
                budget,
                sink,
            } => match sink.finish_block(block, budget) {
                Ok((decoder, scratch, decoded)) => (Mode::Idle(decoder, scratch), Ok(decoded)),
                Err(cause) => (Mode::Failed, Err(cause)),
            },
            Source::Discard(block, _, scratch) => match block.finish() {
                Ok(decoder) => (
                    Mode::Idle(decoder, scratch),
                    Err(conn::Error::HeaderState(conn::HeaderState {
                        expected: conn::HeaderPhase::Store,
                        actual: conn::HeaderPhase::Discard,
                    })),
                ),
                Err(cause) => (Mode::Failed, Err(conn::Error::Hpack(cause))),
            },
        })?;
        let frame = input.prepare(total)?;
        Ok(header::Prepared::new(
            frame,
            event,
            out.head,
            out.validation,
        ))
    }

    pub(crate) fn finish_drop<C: session::Storage, A: rules::Push>(
        &mut self,
        input: &mut input::FrameInput<C>,
        range: ops::Range<usize>,
        total: usize,
    ) -> Result<(), conn::Error> {
        self.feed_range::<C, A>(input, range)?;
        self.decoder.map_active(|active| match active.source {
            Source::Discard(block, _, scratch) => match block.finish() {
                Ok(decoder) => (Mode::Idle(decoder, scratch), Ok(())),
                Err(cause) => (Mode::Failed, Err(conn::Error::Hpack(cause))),
            },
            Source::Store {
                block,
                budget: _,
                sink,
            } => {
                let done = block.finish();
                let scratch = sink.recycle();
                match done {
                    Ok(decoder) => (
                        Mode::Idle(decoder, scratch),
                        Err(conn::Error::HeaderState(conn::HeaderState {
                            expected: conn::HeaderPhase::Discard,
                            actual: conn::HeaderPhase::Store,
                        })),
                    ),
                    Err(cause) => (Mode::Failed, Err(conn::Error::Hpack(cause))),
                }
            }
        })?;
        input
            .bytes
            .try_consume(total)
            .map_err(conn::Error::Consume)?;
        Ok(())
    }

    pub(crate) fn pending(&self) -> Option<Pending> {
        self.decoder.active().map(|active| active.pending)
    }

    pub(crate) fn try_pending(&self) -> Result<Pending, conn::Error> {
        Ok(self.decoder.try_active()?.pending)
    }

    pub(crate) fn try_head_mut(&mut self) -> Result<&mut Pending, conn::Error> {
        Ok(&mut self.decoder.try_mut()?.pending)
    }

    fn decode_all<C: session::Storage, A: rules::Push>(
        &mut self,
        input: &header::Input<'_, '_, C>,
        part: Fragment,
    ) -> Result<output::Decoded<P>, conn::Error> {
        Self::admit(0, part.len, self.header_cap)?;
        let bytes = input.get(part.range())?;
        let lease = self
            .header_pool
            .try_acquire()?
            .ok_or(conn::Error::HeaderDecodeFull {
                used: self.header_slots,
                limit: self.header_slots,
            })?;
        let connect = self.connect;
        self.decoder.map_idle(|decoder, scratch| {
            let sink = match sink::Header::try_new(lease, part.kind, connect, scratch) {
                Ok(sink) => sink,
                Err((scratch, cause)) => {
                    return (
                        Mode::Idle(decoder, scratch),
                        Err(conn::Error::Header(cause)),
                    );
                }
            };
            let (first, rest) = bytes.split();
            let Some(rest) = rest else {
                return match decoder.decode_to::<Decode, _>(first, sink.hpack::<A>()) {
                    Ok((decoder, admit, sink)) => match sink.into_header().finish(admit) {
                        Ok((scratch, decoded)) => (Mode::Idle(decoder, scratch), Ok(decoded)),
                        Err(cause) => (Mode::Failed, Err(cause)),
                    },
                    Err((sink, cause)) => {
                        drop(sink.into_header().recycle());
                        (Mode::Failed, Err(cause.into()))
                    }
                };
            };
            let mut budget = decoder.budget();
            let block = Phase::<Decode>::Representation(decoder);
            let (block, sink) = match block.feed_all(
                &mut budget,
                iter::once(first).chain(rest),
                sink.hpack::<A>(),
            ) {
                Ok((block, sink)) => (block, sink.into_header()),
                Err((sink, cause)) => {
                    drop(sink.into_header().recycle());
                    return (Mode::Failed, Err(cause.into()));
                }
            };
            match sink.finish_block(block, budget) {
                Ok((decoder, scratch, decoded)) => (Mode::Idle(decoder, scratch), Ok(decoded)),
                Err(cause) => (Mode::Failed, Err(cause)),
            }
        })
    }
}

const _: () = {
    assert!(core::mem::size_of::<Kind>() == 8);
    assert!(core::mem::size_of::<Pending>() == 16);
};
