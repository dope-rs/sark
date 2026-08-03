use o3::buffer::{
    Bytes, Retained, SharedLease, SharedPool, SharedPoolLayout, SharedPoolPlan, ValidatedPrefix,
};
use o3::collections::{FixedQueue, FixedQueueVacantEntry};
use sark_core::http::{
    Field, HeadBlock, HeadConsumer, HeadDisposition, HeadPlan, HeadSection, RawHeadPlan,
};
use sark_core::pool::GrowingSharedPool;

use crate::conn::{CLIENT_PREFACE, ConnError, DataPayload, Event};
use crate::frame::{ErrorCode, Flags, FrameHeader, HEADER_LEN, ParseError};
use crate::hpack;
use crate::retained_segments::RetainedSegments;
use crate::stream::StreamId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PendingHeaderAction {
    Deliver,
    Reset(ErrorCode),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PendingKind {
    Headers {
        end_stream: bool,
        trailing: bool,
        action: PendingHeaderAction,
    },
    PushPromise {
        promised: StreamId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PendingHeaders {
    pub(super) stream_id: StreamId,
    pub(super) kind: PendingKind,
    pub(super) continuations: u32,
}

pub(super) struct IngressConfig {
    pub(super) inbound_capacity: usize,
    pub(super) event_capacity: usize,
    pub(super) data_layout: SharedPoolLayout,
    pub(super) header_plan: SharedPoolPlan,
    pub(super) decoder_table_size: usize,
    pub(super) header_cap: usize,
    pub(super) preface_done: bool,
}

struct ActiveHeaders<P: HeadPlan> {
    block: hpack::DecoderBlock,
    sink: HeaderSink<P>,
    wire_len: usize,
}

struct HeaderSink<P: HeadPlan> {
    lease: SharedLease,
    section: HeadSection<P>,
    overflow: bool,
}

pub(super) struct PreparedFrame<'a, F> {
    prefix: ValidatedPrefix<'a, RetainedSegments, F>,
}

pub(super) struct PreparedDataEvent<'a, F, S, B> {
    pub(super) frame: PreparedFrame<'a, F>,
    pub(super) event: FixedQueueVacantEntry<'a, Event<S, B>>,
    pub(super) payload: DataPayload,
}

pub(super) struct PreparedHeadersEvent<'a, F, S, B> {
    frame: PreparedFrame<'a, F>,
    event: FixedQueueVacantEntry<'a, Event<S, B>>,
    headers: B,
    selection: S,
    invalid: bool,
}

impl<F: FnOnce(&mut RetainedSegments, usize)> PreparedFrame<'_, F> {
    pub(super) fn commit(self) {
        self.prefix.commit();
    }

    pub(super) fn reject(self, error: ConnError) -> Result<(), ConnError> {
        self.commit();
        Err(error)
    }
}

impl<F: FnOnce(&mut RetainedSegments, usize), S, B> PreparedHeadersEvent<'_, F, S, B> {
    pub(super) const fn is_invalid(&self) -> bool {
        self.invalid
    }

    pub(super) fn commit_headers(self, stream_id: StreamId, end_stream: bool, trailing: bool) {
        self.frame.commit();
        self.event.push_back(Event::Headers {
            stream_id,
            headers: self.headers,
            selection: self.selection,
            end_stream,
            trailing,
        });
    }

    pub(super) fn commit_push_promise(self, stream_id: StreamId, promised: StreamId) {
        self.frame.commit();
        self.event.push_back(Event::PushPromise {
            stream_id,
            promised_stream_id: promised,
            headers: self.headers,
            selection: self.selection,
        });
    }

    pub(super) fn discard(self) {
        self.frame.commit();
    }
}

impl<P: HeadPlan> HeaderSink<P> {
    fn new(lease: SharedLease, request: bool, trailing: bool) -> Self {
        Self {
            lease,
            section: HeadSection::new(request, trailing),
            overflow: false,
        }
    }

    fn field(&mut self, name: &[u8], value: &[u8]) {
        if self.overflow {
            return;
        }
        let mut writer = self.lease.spare_writer();
        let decision = self.section.disposition(name, None, writer.as_mut_slice());
        if !self
            .section
            .decoded(decision, Field::new(name, value), writer.as_mut_slice())
        {
            return;
        }
        let disposition = decision.disposition;
        let field_start = writer.len();
        let value_start = if P::Block::TAGGED {
            let HeadDisposition::Tagged(tag) = disposition else {
                return;
            };
            let Some(prefix) = tag.prefix(value.len()) else {
                self.overflow = true;
                return;
            };
            self.overflow = writer.try_extend_from_slice(&prefix).is_err();
            field_start + prefix.len()
        } else {
            if matches!(
                disposition,
                HeadDisposition::Discard | HeadDisposition::Skip
            ) {
                return;
            }
            let (Ok(name_len), Ok(value_len)) =
                (u32::try_from(name.len()), u32::try_from(value.len()))
            else {
                self.overflow = true;
                return;
            };
            self.overflow = writer
                .try_extend_from_slice(&name_len.to_ne_bytes())
                .and_then(|()| writer.try_extend_from_slice(&value_len.to_ne_bytes()))
                .and_then(|()| writer.try_extend_from_slice(name))
                .is_err();
            field_start + 8 + name.len()
        };
        if self.overflow {
            return;
        }
        let value_end = value_start + value.len();
        self.overflow = writer.try_extend_from_slice(value).is_err();
        if self.overflow {
            return;
        }
        let retained = writer.as_mut_slice();
        self.section
            .committed(disposition, value_start..value_end, retained);
    }
}

pub(super) struct Ingress<P: HeadPlan = RawHeadPlan> {
    bytes: RetainedSegments,
    events: FixedQueue<Event<P::Selection, P::Block>>,
    data_permits: SharedPool,
    header_pool: GrowingSharedPool,
    decoder: hpack::Decoder,
    active_headers: Option<ActiveHeaders<P>>,
    pending_headers: Option<PendingHeaders>,
    header_cap: usize,
    preface_done: bool,
}

impl<P: HeadPlan> Ingress<P> {
    pub(super) fn from_config(config: IngressConfig) -> Self {
        let IngressConfig {
            inbound_capacity,
            event_capacity,
            data_layout,
            header_plan,
            decoder_table_size,
            header_cap,
            preface_done,
        } = config;
        let mut decoder = hpack::Decoder::new(decoder_table_size);
        decoder.set_max_header_list_size(Some(header_cap));
        let chunk_capacity = data_layout
            .slots()
            .saturating_add(header_plan.max_slots())
            .saturating_add(1);
        Self {
            bytes: RetainedSegments::new(inbound_capacity, chunk_capacity),
            events: FixedQueue::with_capacity(event_capacity),
            data_permits: SharedPool::from_layout(data_layout),
            header_pool: GrowingSharedPool::from_plan(header_plan),
            decoder,
            active_headers: None,
            pending_headers: None,
            header_cap,
            preface_done,
        }
    }

    pub(super) fn append(&mut self, bytes: &[u8]) -> Result<(), ConnError> {
        self.append_retained(Bytes::<Retained>::copy_from_slice(bytes))
    }

    pub(super) fn append_retained(&mut self, bytes: Bytes<Retained>) -> Result<(), ConnError> {
        self.bytes.push(bytes).map_err(|_| ConnError::Overload)
    }

    pub(super) fn accept_preface(&mut self) -> Result<bool, ConnError> {
        if self.preface_done {
            return Ok(true);
        }
        if self.bytes.len() < CLIENT_PREFACE.len() {
            return Ok(false);
        }
        let mut preface = [0; CLIENT_PREFACE.len()];
        if !self.bytes.copy_range_into(0, &mut preface) {
            return Ok(false);
        }
        if preface != CLIENT_PREFACE {
            return Err(ConnError::BadPreface);
        }
        self.ensure_event_capacity()?;
        if !self.bytes.try_consume(CLIENT_PREFACE.len()) {
            return Err(ConnError::FrameSize);
        }
        self.preface_done = true;
        self.push_event(Event::PrefaceComplete)?;
        Ok(true)
    }

    pub(super) fn complete_preface(&mut self) {
        debug_assert!(self.preface_done);
        let inserted = self.events.push_back(Event::PrefaceComplete);
        debug_assert!(inserted.is_ok());
    }

    pub(super) fn next_frame(
        &mut self,
        max_frame_size: u32,
    ) -> Result<Option<FrameHeader>, ConnError> {
        loop {
            let header = match self.parse_frame_header() {
                Ok(header) => header,
                Err(ParseError::NeedMore) => return Ok(None),
                Err(ParseError::BadType(_)) => {
                    let mut prefix = [0; 3];
                    if !self.bytes.copy_range_into(0, &mut prefix) {
                        return Ok(None);
                    }
                    let length = u32::from_be_bytes([0, prefix[0], prefix[1], prefix[2]]);
                    if length > max_frame_size {
                        return Err(ConnError::FrameSize);
                    }
                    let total = HEADER_LEN + length as usize;
                    if self.bytes.len() < total {
                        return Ok(None);
                    }
                    if !self.bytes.try_consume(total) {
                        return Err(ConnError::FrameSize);
                    }
                    continue;
                }
                Err(error) => return Err(ConnError::ParseError(error)),
            };
            if header.length.as_u32() > max_frame_size {
                return Err(ConnError::FrameSize);
            }
            let total = HEADER_LEN + header.length.as_usize();
            return Ok((self.bytes.len() >= total).then_some(header));
        }
    }

    fn parse_frame_header(&self) -> Result<FrameHeader, ParseError> {
        let mut bytes = [0; HEADER_LEN];
        if !self.bytes.copy_range_into(0, &mut bytes) {
            return Err(ParseError::NeedMore);
        }
        FrameHeader::parse(&bytes)
    }

    pub(super) fn unpadded_payload(
        &self,
        header: FrameHeader,
    ) -> Result<(usize, usize), ParseError> {
        let mut start = HEADER_LEN;
        let mut len = header.length.as_usize();
        if !header.flags.has(Flags::PADDED) {
            return Ok((start, len));
        }
        if len == 0 {
            return Err(ParseError::Padding);
        }
        let mut byte = [0; 1];
        let copied = self.bytes.copy_range_into(start, &mut byte);
        debug_assert!(copied);
        let padding = byte[0] as usize;
        if padding + 1 > len {
            return Err(ParseError::Padding);
        }
        start += 1;
        len -= padding + 1;
        Ok((start, len))
    }

    pub(super) fn copy(&self, start: usize, out: &mut [u8]) -> bool {
        self.bytes.copy_range_into(start, out)
    }

    pub(super) fn data(&mut self, start: usize, len: usize) -> Result<DataPayload, ConnError> {
        Self::retain_data(&self.bytes, &self.data_permits, start, len)
    }

    pub(super) fn prepare_frame(
        &mut self,
        total: usize,
    ) -> Result<PreparedFrame<'_, impl FnOnce(&mut RetainedSegments, usize)>, ConnError> {
        let prefix = self
            .bytes
            .prepare_consume(total)
            .map_err(|_| ConnError::FrameSize)?;
        Ok(PreparedFrame { prefix })
    }

    pub(super) fn prepare_data_event(
        &mut self,
        start: usize,
        len: usize,
        total: usize,
    ) -> Result<
        PreparedDataEvent<'_, impl FnOnce(&mut RetainedSegments, usize), P::Selection, P::Block>,
        ConnError,
    > {
        let Self {
            bytes,
            events,
            data_permits,
            ..
        } = self;
        let event = events.vacant_entry().ok_or(ConnError::Overload)?;
        let payload = Self::retain_data(bytes, data_permits, start, len)?;
        let prefix = bytes
            .prepare_consume(total)
            .map_err(|_| ConnError::FrameSize)?;
        Ok(PreparedDataEvent {
            frame: PreparedFrame { prefix },
            event,
            payload,
        })
    }

    fn retain_data(
        bytes: &RetainedSegments,
        data_permits: &SharedPool,
        start: usize,
        len: usize,
    ) -> Result<DataPayload, ConnError> {
        let permit = data_permits
            .try_acquire()
            .ok_or(ConnError::Overload)?
            .freeze();
        let bytes = bytes
            .retained_range(start, len)
            .map_err(|_| ConnError::Overload)?
            .ok_or(ConnError::FrameSize)?;
        Ok(DataPayload::from_retained(bytes, permit))
    }

    pub(super) fn try_consume(&mut self, n: usize) -> Result<(), ConnError> {
        self.bytes
            .try_consume(n)
            .then_some(())
            .ok_or(ConnError::FrameSize)
    }

    pub(super) fn poll_event(&mut self) -> Option<Event<P::Selection, P::Block>> {
        self.events.pop_front()
    }

    pub(super) fn push_event(
        &mut self,
        event: Event<P::Selection, P::Block>,
    ) -> Result<(), ConnError> {
        self.events
            .push_back(event)
            .map_err(|_| ConnError::Overload)
    }

    pub(super) fn ensure_event_capacity(&self) -> Result<(), ConnError> {
        if self.events.is_full() {
            Err(ConnError::Overload)
        } else {
            Ok(())
        }
    }

    pub(super) fn begin_headers(
        &mut self,
        start: usize,
        len: usize,
        request: bool,
        trailing: bool,
    ) -> Result<(), ConnError> {
        debug_assert!(self.active_headers.is_none());
        if self.active_headers.is_some() {
            return Err(ConnError::Continuation);
        }
        if len > self.header_cap {
            return Err(ConnError::HeaderListTooLarge);
        }
        let lease = self.header_pool.try_acquire().ok_or(ConnError::Overload)?;
        let mut active = ActiveHeaders {
            block: self.decoder.start_block(),
            sink: HeaderSink::new(lease, request, trailing),
            wire_len: len,
        };
        Self::decode_range(
            &self.bytes,
            &mut self.decoder,
            &mut active.block,
            &mut active.sink,
            start,
            len,
        )?;
        self.active_headers = Some(active);
        Ok(())
    }

    pub(super) fn prepare_headers_event(
        &mut self,
        start: usize,
        len: usize,
        total: usize,
        request: bool,
        trailing: bool,
    ) -> Result<
        PreparedHeadersEvent<'_, impl FnOnce(&mut RetainedSegments, usize), P::Selection, P::Block>,
        ConnError,
    > {
        let Self {
            bytes,
            events,
            header_pool,
            decoder,
            header_cap,
            ..
        } = self;
        let event = events.vacant_entry().ok_or(ConnError::Overload)?;
        let (headers, selection, invalid) = Self::decode_complete_headers(
            bytes,
            decoder,
            header_pool,
            *header_cap,
            start,
            len,
            request,
            trailing,
        )?;
        let prefix = bytes
            .prepare_consume(total)
            .map_err(|_| ConnError::FrameSize)?;
        Ok(PreparedHeadersEvent {
            frame: PreparedFrame { prefix },
            event,
            headers,
            selection,
            invalid,
        })
    }

    fn decode_complete_headers(
        bytes: &RetainedSegments,
        decoder: &mut hpack::Decoder,
        header_pool: &mut GrowingSharedPool,
        header_cap: usize,
        start: usize,
        len: usize,
        request: bool,
        trailing: bool,
    ) -> Result<(P::Block, P::Selection, bool), ConnError> {
        if len > header_cap {
            return Err(ConnError::HeaderListTooLarge);
        }
        let lease = header_pool.try_acquire().ok_or(ConnError::Overload)?;
        let mut sink = HeaderSink::new(lease, request, trailing);
        if let Some(fragment) = bytes.contiguous_range(start, len) {
            let over_limit = decoder.decode_bounded(fragment, |name, value| {
                sink.field(name, value);
            })?;
            return Self::finish_sink(over_limit, sink);
        }
        let mut block = decoder.start_block();
        Self::decode_range(bytes, decoder, &mut block, &mut sink, start, len)?;
        Self::finish_decoded(block, sink)
    }

    pub(super) fn continue_headers(&mut self, start: usize, len: usize) -> Result<(), ConnError> {
        let active = self
            .active_headers
            .as_mut()
            .ok_or(ConnError::Continuation)?;
        if len > self.header_cap.saturating_sub(active.wire_len) {
            return Err(ConnError::HeaderListTooLarge);
        }
        active.wire_len += len;
        Self::decode_range(
            &self.bytes,
            &mut self.decoder,
            &mut active.block,
            &mut active.sink,
            start,
            len,
        )
    }

    pub(super) fn prepare_continued_headers_event(
        &mut self,
        start: usize,
        len: usize,
        total: usize,
    ) -> Result<
        (
            PreparedHeadersEvent<
                '_,
                impl FnOnce(&mut RetainedSegments, usize),
                P::Selection,
                P::Block,
            >,
            PendingHeaders,
        ),
        ConnError,
    > {
        let Self {
            bytes,
            events,
            decoder,
            active_headers,
            pending_headers,
            header_cap,
            ..
        } = self;
        let event = events.vacant_entry().ok_or(ConnError::Overload)?;
        let active = active_headers.as_mut().ok_or(ConnError::Continuation)?;
        if len > header_cap.saturating_sub(active.wire_len) {
            return Err(ConnError::HeaderListTooLarge);
        }
        active.wire_len += len;
        Self::decode_range(
            bytes,
            decoder,
            &mut active.block,
            &mut active.sink,
            start,
            len,
        )?;
        let active = active_headers.take().ok_or(ConnError::Continuation)?;
        let pending = pending_headers.take().ok_or(ConnError::Continuation)?;
        let (headers, selection, invalid) = Self::finish_decoded(active.block, active.sink)?;
        let prefix = bytes
            .prepare_consume(total)
            .map_err(|_| ConnError::FrameSize)?;
        Ok((
            PreparedHeadersEvent {
                frame: PreparedFrame { prefix },
                event,
                headers,
                selection,
                invalid,
            },
            pending,
        ))
    }

    fn decode_range(
        bytes: &RetainedSegments,
        decoder: &mut hpack::Decoder,
        block: &mut hpack::DecoderBlock,
        sink: &mut HeaderSink<P>,
        start: usize,
        len: usize,
    ) -> Result<(), ConnError> {
        if let Some(fragment) = bytes.contiguous_range(start, len) {
            decoder.decode_fragment(block, fragment, |name, value| {
                sink.field(name, value);
            })?;
            return Ok(());
        }
        let mut decode_error = None;
        let available = bytes.for_each_range(start, len, |fragment| {
            if decode_error.is_none() {
                decode_error = decoder
                    .decode_fragment(block, fragment, |name, value| {
                        sink.field(name, value);
                    })
                    .err();
            }
        });
        if !available {
            return Err(ConnError::FrameSize);
        }
        if let Some(error) = decode_error {
            return Err(error.into());
        }
        Ok(())
    }

    fn finish_decoded(
        block: hpack::DecoderBlock,
        sink: HeaderSink<P>,
    ) -> Result<(P::Block, P::Selection, bool), ConnError> {
        let over_limit = block.finish()?;
        Self::finish_sink(over_limit, sink)
    }

    fn finish_sink(
        over_limit: bool,
        sink: HeaderSink<P>,
    ) -> Result<(P::Block, P::Selection, bool), ConnError> {
        let HeaderSink {
            lease,
            section,
            overflow,
        } = sink;
        if overflow {
            return Err(ConnError::HeaderListTooLarge);
        }
        let (selection, valid) = section.finish();
        Ok((
            P::Block::from_pooled(lease.freeze()),
            selection,
            over_limit || !valid,
        ))
    }

    pub(super) fn has_pending_headers(&self) -> bool {
        self.pending_headers.is_some()
    }

    pub(super) fn start_pending_headers(&mut self, pending: PendingHeaders) {
        self.pending_headers = Some(pending);
    }

    pub(super) fn pending_headers_mut(&mut self) -> Option<&mut PendingHeaders> {
        self.pending_headers.as_mut()
    }
}
