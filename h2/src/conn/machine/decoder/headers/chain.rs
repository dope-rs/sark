use core::{iter, ops};

use conn::{
    event, ingress,
    ingress::{events, headers, input},
    registry,
};
use decoder::{Feed, Refill};
use o3::buffer::write::consume::Consume as _;
use registry::store;
use sark_core::life::session;
use sark_http::{self, field::metadata::section::rules};

use crate::{
    conn,
    conn::machine::{
        self, decoder,
        decoder::headers::{Outcome, complete, promise},
        ops::state,
    },
    frame,
    frame::{layout, numbers},
    stream,
};

enum Read {
    Refill(usize),
    Complete {
        range: ops::Range<usize>,
        total: usize,
        count: headers::Continuation,
        consumed: usize,
    },
}

impl<R, F, C, A, E> decoder::Decoder<'_, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub(in crate::conn::machine::decoder) fn try_feed<T: AsRef<[u8]>>(
        &mut self,
        max: numbers::Limit,
        input: T,
    ) -> Result<Feed<T>, conn::Error> {
        let bytes = input.as_ref();
        if self.conn.ingress.headers.pending().is_none()
            || !self.conn.ingress.input.bytes.is_empty()
        {
            return Ok(Feed::Pending(input));
        }
        let Some((head, part)) = bytes.split_first_chunk::<{ frame::HEADER_LEN }>() else {
            return Ok(Feed::Pending(input));
        };
        let header = layout::Header::read(head)
            .map_err(|error| conn::Error::Continuation(conn::Continuation::Type(error.actual)))?;
        let total = input::frame_len(header.kind as u8, header.length, max)?;
        if total != bytes.len() {
            return Ok(Feed::Pending(input));
        }
        let head = self.conn.ingress.headers.try_head_mut()?;
        let chain = continuation_frame(header, head.stream_id)?;
        if matches!(chain, frame::Block::End) {
            return Ok(Feed::Pending(input));
        }
        let count = head.continuations.next()?;
        head.continuations = count;
        self.conn.ingress.headers.feed_direct::<A>(part)?;
        Ok(Feed::Consumed)
    }

    pub(in crate::conn::machine::decoder) fn run<I, X>(
        &mut self,
        max: numbers::Limit,
        source: &mut I,
    ) -> Result<(), conn::Error>
    where
        I: Iterator<Item = Result<C, X>>,
        conn::Error: From<X>,
    {
        let head = self.conn.ingress.headers.try_pending()?;
        let stream = head.stream_id;
        let id = stream.get();
        let kind = head.kind;
        'input: loop {
            if self.conn.ingress.input.bytes.is_empty() {
                let owner = match source.next() {
                    Some(owner) => owner?,
                    None => return Ok(()),
                };
                let owner = match self.try_feed(max, owner)? {
                    Feed::Consumed => continue 'input,
                    Feed::Pending(owner) => owner,
                };
                match self.conn.ingress.input.push(owner) {
                    Ok(()) => {}
                    Err(full) => return Err(full.reject()),
                }
            }
            let read = {
                let mut offset = 0usize;
                let ingress::Ingress {
                    input,
                    headers: blocks,
                    ..
                } = &mut self.conn.ingress;
                let mut scan = input::Scan::new(&input.bytes);
                loop {
                    let (header, total) = match scan.read(offset, max)? {
                        Some(header) => header,
                        None => break Read::Refill(offset),
                    };
                    let chain = continuation_frame(header, stream)?;
                    let count = blocks.try_pending()?.continuations.next()?;
                    let start = offset + frame::HEADER_LEN;
                    let range = start..total;
                    if matches!(chain, frame::Block::End) {
                        break Read::Complete {
                            range,
                            total,
                            count,
                            consumed: offset,
                        };
                    }
                    blocks.try_head_mut()?.continuations = count;
                    blocks.feed_range::<C, A>(input, range)?;
                    offset = total;
                }
            };
            let (range, total, count, offset) = match read {
                Read::Complete {
                    range,
                    total,
                    count,
                    consumed,
                } => (range, total, count, consumed),
                Read::Refill(offset) => {
                    self.conn
                        .ingress
                        .input
                        .bytes
                        .try_consume(offset)
                        .map_err(conn::Error::Consume)?;
                    match self.refill(source)? {
                        Refill::Added => continue 'input,
                        Refill::Exhausted => return Ok(()),
                    }
                }
            };

            let emit = match kind {
                headers::Kind::Headers {
                    action: headers::Action::Deliver,
                    ..
                }
                | headers::Kind::PushPromise { .. } => true,
                headers::Kind::Headers {
                    action: headers::Action::Reset(_),
                    ..
                } => self.conn.state.active_streams.store.key(stream).is_some(),
            };
            if emit {
                match self.ensure_event() {
                    Ok(()) => {}
                    Err(error) => {
                        self.conn
                            .ingress
                            .input
                            .bytes
                            .try_consume(offset)
                            .map_err(conn::Error::Consume)?;
                        return Err(error);
                    }
                }
            }
            self.conn.ingress.headers.try_head_mut()?.continuations = count;

            let machine::Connection {
                ingress: recv,
                egress,
                state,
                ..
            } = &mut *self.conn;
            let state::State {
                active_streams: active,
                stream_history: past,
                lifecycle: life,
                ..
            } = &mut *state;
            let ingress::Ingress {
                input,
                events,
                headers: blocks,
                ..
            } = &mut *recv;
            return match kind {
                headers::Kind::Headers {
                    action: headers::Action::Reset(error),
                    ..
                } => {
                    match active.visit(
                        past,
                        stream,
                        (),
                        |key, _, ()| -> Result<_, conn::Error> {
                            let slot = events.vacant()?;
                            blocks.finish_drop::<C, A>(input, range.clone(), total)?;
                            Ok(store::Action::Reset(
                                error,
                                life.keeps_events(),
                                (key, events::Reset::new(slot, None)),
                            ))
                        },
                    )? {
                        Ok((key, notify)) => state.finish_reset(egress, key, error, Some(notify)),
                        Err((_, ())) => {
                            blocks.finish_drop::<C, A>(input, range, total)?;
                            egress.outbound.reset(id, error)
                        }
                    }
                }
                headers::Kind::Headers {
                    boundary,
                    section,
                    action: headers::Action::Deliver,
                } => {
                    let ready = blocks.resume::<C, A, E>(input, events, range, total)?;
                    let result = active.visit(past, stream, ready, |key, record, ready| {
                        complete::<R, _, _, _>(
                            key,
                            record,
                            ready,
                            boundary,
                            section,
                            life.keeps_events(),
                        )
                    })?;
                    match result {
                        Ok(Outcome::Done) => Ok(()),
                        Ok(Outcome::Reset(key, error, notify)) => {
                            state.finish_reset(egress, key, error, notify)
                        }
                        Err((past, ready)) => {
                            ready.discard();
                            match past {
                                registry::Past::Closed => Ok(()),
                                registry::Past::Idle => {
                                    Err(conn::Error::Protocol(conn::Protocol::Idle(id)))
                                }
                            }
                        }
                    }
                }
                headers::Kind::PushPromise {
                    promised: push,
                    associated: assoc,
                } => {
                    let ready = blocks.resume::<C, A, E>(input, events, range, total)?;
                    promise::Promise::new(stream, push).commit::<R, F, C, E>(
                        assoc,
                        &mut egress.outbound,
                        state,
                        ready,
                    )
                }
            };
        }
    }

    pub(in crate::conn::machine::decoder) fn flush(
        &mut self,
        max: numbers::Limit,
        full: conn::Error,
    ) -> Result<(), conn::Error> {
        if self.conn.ingress.headers.pending().is_none() {
            return Err(full);
        }
        self.run(max, &mut iter::empty::<Result<C, conn::Error>>())
    }
}

fn continuation_frame(
    header: layout::Header,
    stream: stream::Nonzero,
) -> Result<frame::Block, conn::Error> {
    use numbers::{Flags, Type};

    let id = stream.get();

    if header.kind != Type::Continuation {
        return Err(conn::Error::Continuation(conn::Continuation::Type(
            header.kind as u8,
        )));
    }
    if header.stream_id != id {
        return Err(conn::Error::Continuation(conn::Continuation::Stream {
            expected: id,
            actual: header.stream_id,
        }));
    }
    let len = header.length.into_usize();
    let chain = if header.flags.contains(Flags::END_HEADERS) {
        frame::Block::End
    } else {
        frame::Block::More
    };
    if len == 0 && matches!(chain, frame::Block::More) {
        return Err(conn::Error::Continuation(conn::Continuation::Empty(
            header.stream_id,
        )));
    }
    Ok(chain)
}
