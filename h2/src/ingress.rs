use o3::buffer::{Bytes, Retained, SharedLease, SharedPool, SharedPoolLayout, SharedPoolPlan};
use o3::collections::FixedQueue;

use crate::conn::{CLIENT_PREFACE, ConnError, DataPayload, Event};
use crate::frame::{Flags, FrameHeader, HEADER_LEN, ParseError};
use crate::growing_pool::GrowingSharedPool;
use crate::hpack;
use crate::retained_segments::RetainedSegments;
use crate::stream::StreamId;
use crate::validate::{HeaderKind, Validate};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PendingKind {
    Headers { end_stream: bool, trailing: bool },
    PushPromise { promised: StreamId },
}

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

struct ActiveHeaders {
    block: hpack::DecoderBlock,
    sink: HeaderSink,
    wire_len: usize,
}

struct HeaderSink {
    lease: SharedLease,
    validation: Validate,
    overflow: bool,
}

impl HeaderSink {
    fn new<K: HeaderKind>(lease: SharedLease, trailing: bool) -> Self {
        Self {
            lease,
            validation: Validate::new::<K>(trailing),
            overflow: false,
        }
    }

    fn field(&mut self, name: &[u8], value: &[u8]) {
        if self.overflow || !self.validation.field(name, value) {
            return;
        }
        let Ok(name_len) = u32::try_from(name.len()) else {
            self.overflow = true;
            return;
        };
        let Ok(value_len) = u32::try_from(value.len()) else {
            self.overflow = true;
            return;
        };
        let mut writer = self.lease.spare_writer();
        self.overflow = writer
            .try_extend_from_slice(&name_len.to_ne_bytes())
            .and_then(|()| writer.try_extend_from_slice(&value_len.to_ne_bytes()))
            .and_then(|()| writer.try_extend_from_slice(name))
            .and_then(|()| writer.try_extend_from_slice(value))
            .is_err();
    }
}

pub(super) struct Ingress {
    bytes: RetainedSegments,
    events: FixedQueue<Event>,
    data_permits: SharedPool,
    header_pool: GrowingSharedPool,
    decoder: hpack::Decoder,
    active_headers: Option<ActiveHeaders>,
    pending_headers: Option<PendingHeaders>,
    header_cap: usize,
    preface_done: bool,
}

impl Ingress {
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
        let permit = self
            .data_permits
            .try_acquire()
            .ok_or(ConnError::Overload)?
            .freeze();
        let bytes = self
            .bytes
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

    pub(super) fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    pub(super) fn push_event(&mut self, event: Event) -> Result<(), ConnError> {
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

    pub(super) fn begin_headers<K: HeaderKind>(
        &mut self,
        start: usize,
        len: usize,
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
            sink: HeaderSink::new::<K>(lease, trailing),
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

    pub(super) fn complete_headers<K: HeaderKind>(
        &mut self,
        start: usize,
        len: usize,
        trailing: bool,
    ) -> Result<(hpack::HeaderBlock, bool), ConnError> {
        if len > self.header_cap {
            return Err(ConnError::HeaderListTooLarge);
        }
        let lease = self.header_pool.try_acquire().ok_or(ConnError::Overload)?;
        let mut sink = HeaderSink::new::<K>(lease, trailing);
        if let Some(fragment) = self.bytes.contiguous_range(start, len) {
            let over_limit = self.decoder.decode_bounded(fragment, |name, value| {
                sink.field(name, value);
            })?;
            return Self::finish_sink(over_limit, sink);
        }
        let mut block = self.decoder.start_block();
        Self::decode_range(
            &self.bytes,
            &mut self.decoder,
            &mut block,
            &mut sink,
            start,
            len,
        )?;
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

    pub(super) fn finish_headers(&mut self) -> Result<(hpack::HeaderBlock, bool), ConnError> {
        let active = self.active_headers.take().ok_or(ConnError::Continuation)?;
        Self::finish_decoded(active.block, active.sink)
    }

    fn decode_range(
        bytes: &RetainedSegments,
        decoder: &mut hpack::Decoder,
        block: &mut hpack::DecoderBlock,
        sink: &mut HeaderSink,
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
        sink: HeaderSink,
    ) -> Result<(hpack::HeaderBlock, bool), ConnError> {
        let over_limit = block.finish()?;
        Self::finish_sink(over_limit, sink)
    }

    fn finish_sink(
        over_limit: bool,
        sink: HeaderSink,
    ) -> Result<(hpack::HeaderBlock, bool), ConnError> {
        if sink.overflow {
            return Err(ConnError::HeaderListTooLarge);
        }
        Ok((
            hpack::HeaderBlock::from_pooled(sink.lease.freeze()),
            over_limit || !sink.validation.finish(),
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

    pub(super) fn take_pending_headers(&mut self) -> Option<PendingHeaders> {
        self.pending_headers.take()
    }
}
