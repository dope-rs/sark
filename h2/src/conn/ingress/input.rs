use std::num::NonZero;

use o3::{
    alloc,
    buffer::{
        self,
        hold::{self, scope},
        resident,
        segments::{self, queue},
        storage,
        write::consume::{self, Consume as _},
    },
    mem,
};
use sark_core::life::session;

use crate::{
    conn,
    conn::{
        event,
        ingress::{self, events, payloads},
    },
    frame::{self, layout, numbers},
};

pub(in crate::conn) fn frame_len(
    kind: u8,
    length: numbers::Length,
    max: numbers::Limit,
) -> Result<usize, conn::Error> {
    if length.into_u32() > max.into_u32() {
        return Err(conn::Error::FrameLimit {
            kind,
            actual: length.into_usize(),
            limit: max.into_usize(),
        });
    }
    Ok(frame::HEADER_LEN + length.into_usize())
}

pub(crate) enum Frame<'a, C: session::Storage> {
    Queued(segments::Drain<'a, queue::Fifo<C>>),
    Headers(&'a mut bool),
    Borrowed {
        consumed: &'a mut usize,
        total: usize,
    },
}

pub(crate) struct Data<'a, S, B, C: session::Storage, E: event::Evidence> {
    pub(crate) frame: Frame<'a, C>,
    pub(crate) event: events::Vacant<'a, S, B, C, E>,
    pub(crate) payload: event::DataPayload<C>,
}

pub(crate) struct FrameInput<C: session::Storage = storage::Shared> {
    pub(crate) bytes: queue::Fifo<C>,
    pub(in crate::conn) retention: hold::Retention<C>,
    payloads: payloads::Store<C>,
    preface: ingress::PrefacePhase,
}

pub(crate) struct Full<C>(mem::Rejected<C, queue::Error>);

pub(crate) enum Append<C> {
    Full(Full<C>),
    Copy(hold::CopyError),
}

impl<C> Full<C> {
    pub(crate) fn into_parts(self) -> (C, conn::Error) {
        let (bytes, error) = self.0.into_parts();
        (bytes, conn::Error::InputFull(error))
    }

    pub(crate) fn reject(self) -> conn::Error {
        conn::Error::InputFull(self.0.discard())
    }
}

pub(crate) enum Preface {
    Established,
    Progress,
    Missing,
    Settings(layout::Header),
}

impl<C: session::Storage> Frame<'_, C> {
    pub(crate) fn commit(self) {
        match self {
            Self::Queued(prefix) => prefix.commit(),
            Self::Headers(committed) => *committed = true,
            Self::Borrowed { consumed, total } => *consumed = total,
        }
    }

    pub(crate) fn reject(self, error: conn::Error) -> Result<(), conn::Error> {
        self.commit();
        Err(error)
    }
}

/// The DATA transaction reads, retains, and commits through one exact input.
pub(crate) trait Source<C: session::Storage> {
    fn copy<const N: usize>(&self, start: usize, target: &mut [u8; N]) -> Result<(), conn::Error>;

    fn retain_data(&self, start: usize, len: usize) -> Result<event::DataPayload<C>, conn::Error>;

    fn prepare(&mut self, total: usize) -> Result<Frame<'_, C>, conn::Error>;

    fn unpadded(&self, header: layout::Header) -> Result<(usize, usize), conn::Error> {
        use numbers::Flags;
        let start = frame::HEADER_LEN;
        let len = header.length.into_usize();
        if !header.flags.contains(Flags::PADDED) {
            return Ok((start, len));
        }
        if len == 0 {
            return Err(conn::Error::ParseError(numbers::ParseError::Padding {
                actual: 0,
                required: 1,
            }));
        }
        let mut byte = [0; 1];
        self.copy(start, &mut byte)?;
        let required = byte[0] as usize + 1;
        if required > len {
            return Err(conn::Error::ParseError(numbers::ParseError::Padding {
                actual: len,
                required,
            }));
        }
        Ok((start + 1, len - required))
    }

    fn prepare_data<'a, S, B, E: event::Evidence>(
        &'a mut self,
        events: &'a mut events::Events<S, B, C, E>,
        start: usize,
        len: usize,
        total: usize,
    ) -> Result<Data<'a, S, B, C, E>, conn::Error> {
        let event = events.vacant()?;
        let payload = self.retain_data(start, len)?;
        let frame = self.prepare(total)?;
        Ok(Data {
            frame,
            event,
            payload,
        })
    }
}

impl<C: session::Storage> FrameInput<C> {
    pub(in crate::conn) fn copy_frame<const N: usize>(
        &self,
        header: layout::Header,
        target: &mut [u8; N],
    ) -> Result<(), conn::Error> {
        if header.length.into_usize() != N {
            return Err(conn::Error::ParseError(numbers::ParseError::FrameSize {
                kind: header.kind,
                actual: header.length.into_usize(),
                expected: N,
            }));
        }
        self.copy(frame::HEADER_LEN, target)
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.preface == ingress::PrefacePhase::Established
    }

    pub(super) fn try_new(
        capacity: queue::Capacity,
        plan: payloads::Plan,
        preface: ingress::PrefacePhase,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            bytes: queue::Fifo::try_with(capacity)?,
            retention: hold::Retention::default(),
            payloads: payloads::Store::try_new(plan)?,
            preface,
        })
    }

    pub(crate) fn append(&mut self, bytes: &[u8]) -> Result<(), Append<C>> {
        let bytes = match self.retention.try_copy(bytes) {
            Ok(bytes) => bytes,
            Err(error) => return Err(Append::Copy(error)),
        };
        self.push(bytes).map_err(Append::Full)
    }

    pub(crate) fn push(&mut self, bytes: C) -> Result<(), Full<C>> {
        self.bytes.try_push(bytes).map_err(Full)
    }

    /// The next wire prefix after the decoder stopped needing more input.
    /// Oversized frames admit only their header, leaving error classification
    /// to the same decoder before any declared payload is retained.
    pub(crate) fn wire_prefix(
        &self,
        incoming: &[u8],
        max: numbers::Limit,
    ) -> Result<NonZero<usize>, conn::Error> {
        let have = self.bytes.len();
        let total = if self.preface == ingress::PrefacePhase::Magic {
            conn::CLIENT_PREFACE.len()
        } else {
            let prefix = self.bytes.prefix(3);
            let mut bytes = prefix.fragments().flatten().chain(incoming.iter()).copied();
            match (bytes.next(), bytes.next(), bytes.next()) {
                (Some(high), Some(middle), Some(low)) => {
                    let length = numbers::Length::from_wire(high, middle, low);
                    if length.into_u32() > max.into_u32() {
                        frame::HEADER_LEN
                    } else {
                        frame::HEADER_LEN + length.into_usize()
                    }
                }
                _ => frame::HEADER_LEN,
            }
        };
        let count = total.saturating_sub(have).min(incoming.len());
        NonZero::new(count).ok_or(conn::Error::InputRange {
            offset: have,
            need: 1,
            have: total,
        })
    }

    pub(crate) fn poll_preface<S, B, E: event::Evidence>(
        &mut self,
        events: &mut events::Events<S, B, C, E>,
        max: numbers::Limit,
    ) -> Result<Preface, conn::Error> {
        use crate::conn::CLIENT_PREFACE;
        match self.preface {
            ingress::PrefacePhase::Established => Ok(Preface::Established),
            ingress::PrefacePhase::Magic => {
                let mut preface = [0; CLIENT_PREFACE.len()];
                let Some(source) = self.read()? else {
                    return Ok(Preface::Missing);
                };
                source.copy(&mut preface);
                conn::preface::Prefix::default()
                    .read(&preface)
                    .map_err(|cause| {
                        let (_, offset, expected, actual) = cause.into_parts();
                        conn::Error::BadPreface {
                            offset,
                            expected,
                            actual,
                        }
                    })?;
                let event = events.vacant()?;
                self.bytes
                    .try_consume(CLIENT_PREFACE.len())
                    .map_err(conn::Error::Consume)?;
                self.preface = ingress::PrefacePhase::InitialSettings;
                event.push_back(event::Event::PrefaceComplete);
                Ok(Preface::Progress)
            }
            ingress::PrefacePhase::InitialSettings => {
                let Some(header) = self.header()? else {
                    return Ok(Preface::Missing);
                };
                let total = frame_len(header.kind as u8, header.length, max)?;
                if self.bytes.len() < total {
                    return Ok(Preface::Missing);
                }
                Ok(Preface::Settings(header))
            }
        }
    }

    pub(crate) fn establish(&mut self) {
        self.preface = ingress::PrefacePhase::Established;
    }

    pub(crate) fn next_frame(
        &mut self,
        max: numbers::Limit,
    ) -> Result<Option<layout::Header>, conn::Error> {
        loop {
            let Some(source) = self.read()? else {
                return Ok(None);
            };
            let header = match layout::Header::assemble(source) {
                Ok(header) => header,
                Err(layout::Unknown { length, actual }) => {
                    let total = frame_len(actual, length, max)?;
                    let Ok(prefix) = segments::Drain::try_new(&mut self.bytes, total) else {
                        return Ok(None);
                    };
                    prefix.commit();
                    continue;
                }
            };
            let total = frame_len(header.kind as u8, header.length, max)?;
            return Ok((self.bytes.len() >= total).then_some(header));
        }
    }

    fn read<const N: usize>(&self) -> Result<Option<queue::Array<'_, C, N>>, conn::Error> {
        match queue::Array::try_new(&self.bytes.cursor(), 0) {
            Ok(source) => Ok(Some(source)),
            Err(bounds) if bounds.ceiling < bounds.amount => Ok(None),
            Err(bounds) => Err(conn::Error::Input(bounds)),
        }
    }

    fn header(&self) -> Result<Option<layout::Header>, conn::Error> {
        let Some(source) = self.read()? else {
            return Ok(None);
        };
        layout::Header::assemble(source)
            .map(Some)
            .map_err(|error| conn::Error::ParseError(numbers::ParseError::BadType(error.actual)))
    }
}

impl<C: session::Storage> Source<C> for FrameInput<C> {
    fn copy<const N: usize>(&self, start: usize, target: &mut [u8; N]) -> Result<(), conn::Error> {
        queue::Array::try_new(&self.bytes.cursor(), start)
            .map_err(conn::Error::Input)?
            .copy(target);
        Ok(())
    }

    fn retain_data(&self, start: usize, len: usize) -> Result<event::DataPayload<C>, conn::Error> {
        let have = self.bytes.len();
        let end = start.checked_add(len).ok_or(conn::Error::InputRange {
            offset: start,
            need: len,
            have,
        })?;
        let bytes = self.bytes.get(start..end).ok_or(conn::Error::InputRange {
            offset: start,
            need: len,
            have,
        })?;
        self.payloads.try_retain(bytes, &self.retention)
    }

    fn prepare(&mut self, total: usize) -> Result<Frame<'_, C>, conn::Error> {
        let prefix =
            segments::Drain::try_new(&mut self.bytes, total).map_err(conn::Error::Consume)?;
        Ok(Frame::Queued(prefix))
    }
}

pub(crate) struct Borrowed<'a, 'input, 'd, R: buffer::Source, B> {
    source: &'a scope::Input<'input, 'd, R, B>,
    bytes: &'input [u8],
    offset: usize,
    consumed: &'a mut usize,
    payloads: &'a payloads::Store<resident::Shared<'d>>,
}

impl<'d> FrameInput<resident::Shared<'d>> {
    pub(crate) fn borrowed<'a, 'input, R: buffer::Cursor, B>(
        &'a self,
        source: &'a scope::Input<'input, 'd, R, B>,
        offset: usize,
        consumed: &'a mut usize,
    ) -> Result<Borrowed<'a, 'input, 'd, R, B>, conn::Error> {
        let bytes = source
            .as_slice()
            .get(offset..)
            .ok_or(conn::Error::InputRange {
                offset,
                need: 0,
                have: source.as_slice().len(),
            })?;
        Ok(Borrowed {
            source,
            bytes,
            offset,
            consumed,
            payloads: &self.payloads,
        })
    }
}

impl<'d, R: buffer::Cursor + hold::raw::Retain<'d>, B> Source<resident::Shared<'d>>
    for Borrowed<'_, '_, 'd, R, B>
{
    fn copy<const N: usize>(&self, start: usize, target: &mut [u8; N]) -> Result<(), conn::Error> {
        let bytes = self
            .bytes
            .get(start..)
            .and_then(<[u8]>::first_chunk::<N>)
            .ok_or(conn::Error::InputRange {
                offset: start,
                need: N,
                have: self.bytes.len(),
            })?;
        *target = *bytes;
        Ok(())
    }

    fn retain_data(
        &self,
        start: usize,
        len: usize,
    ) -> Result<event::DataPayload<resident::Shared<'d>>, conn::Error> {
        let end = start
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(conn::Error::InputRange {
                offset: start,
                need: len,
                have: self.bytes.len(),
            })?;
        let source = self
            .source
            .try_plan(self.offset + start..self.offset + end)
            .map_err(hold::Error::from)?;
        self.payloads
            .try_scoped(source.bind(crate::INGRESS_RETENTION))
    }

    fn prepare(&mut self, total: usize) -> Result<Frame<'_, resident::Shared<'d>>, conn::Error> {
        consume::Error::check(total, self.bytes.len()).map_err(conn::Error::Consume)?;
        Ok(Frame::Borrowed {
            consumed: self.consumed,
            total: self.offset + total,
        })
    }
}

/// A header scan bound to the same queue for ranges, reads, and progress.
pub(crate) struct Scan<'a, C> {
    cursor: queue::Cursor<'a, C>,
}

impl<'a, C: session::Storage> Scan<'a, C> {
    pub(crate) fn new(source: &'a queue::Fifo<C>) -> Self {
        Self {
            cursor: source.cursor(),
        }
    }

    pub(crate) fn read(
        &mut self,
        offset: usize,
        max: numbers::Limit,
    ) -> Result<Option<(layout::Header, usize)>, conn::Error> {
        let source = match queue::Array::try_new(&self.cursor, offset) {
            Ok(source) => source,
            Err(bounds)
                if bounds.offset >= bounds.floor
                    && bounds.offset <= usize::MAX - frame::HEADER_LEN
                    && (bounds.offset > bounds.ceiling
                        || bounds.amount > bounds.ceiling - bounds.offset) =>
            {
                return Ok(None);
            }
            Err(bounds) => return Err(conn::Error::Input(bounds)),
        };
        let header = layout::Header::assemble(source)
            .map_err(|error| conn::Error::Continuation(conn::Continuation::Type(error.actual)))?;
        let total = frame_len(header.kind as u8, header.length, max)?;
        let end = offset.checked_add(total).ok_or(conn::Error::InputRange {
            offset,
            need: total,
            have: self.cursor.len(),
        })?;
        match self.cursor.seek(end) {
            Ok(()) => Ok(Some((header, end))),
            Err(bounds) if bounds.offset > bounds.ceiling => Ok(None),
            Err(bounds) => Err(conn::Error::Input(bounds)),
        }
    }
}
