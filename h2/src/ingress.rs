use o3::buffer::{ByteRing, Pooled, SharedPool};
use o3::collections::FixedQueue;

use crate::conn::{CLIENT_PREFACE, ConnError, Event};
use crate::frame::{Flags, FrameHeader, HEADER_LEN, ParseError};
use crate::hpack;
use crate::stream::StreamId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum PendingKind {
    Headers { end_stream: bool, trailing: bool },
    PushPromise { promised: StreamId },
}

pub(super) struct PendingHeaders {
    pub(super) stream_id: StreamId,
    pub(super) kind: PendingKind,
    pub(super) continuations: u32,
}

pub(super) struct Ingress {
    bytes: ByteRing,
    events: FixedQueue<Event>,
    data_pool: SharedPool,
    header_pool: SharedPool,
    header_block: Vec<u8>,
    decoder: hpack::Decoder,
    pending_headers: Option<PendingHeaders>,
    header_cap: usize,
    preface_done: bool,
}

impl Ingress {
    pub(super) fn new(
        inbound_capacity: usize,
        event_capacity: usize,
        data_capacity: usize,
        data_len: usize,
        header_capacity: usize,
        decoder_table_size: usize,
        header_cap: usize,
        preface_done: bool,
    ) -> Self {
        let mut decoder = hpack::Decoder::new(decoder_table_size);
        decoder.set_max_header_list_size(Some(header_cap));
        Self {
            bytes: ByteRing::with_capacity(inbound_capacity),
            events: FixedQueue::with_capacity(event_capacity),
            data_pool: SharedPool::new(data_capacity, data_len),
            header_pool: SharedPool::new(header_capacity, header_cap),
            header_block: Vec::with_capacity(header_cap),
            decoder,
            pending_headers: None,
            header_cap,
            preface_done,
        }
    }

    pub(super) fn append(&mut self, bytes: &[u8]) -> Result<(), ConnError> {
        if bytes.len() > self.bytes.remaining() {
            return Err(ConnError::Overload);
        }
        self.bytes
            .try_extend_from_slice(bytes)
            .map_err(|_| ConnError::Overload)
    }

    pub(super) fn accept_preface(&mut self) -> Result<bool, ConnError> {
        if self.preface_done {
            return Ok(true);
        }
        if self.bytes.len() < CLIENT_PREFACE.len() {
            return Ok(false);
        }
        let (first, second) = self
            .bytes
            .range_slices(0, CLIENT_PREFACE.len())
            .ok_or(ConnError::BadPreface)?;
        if first != &CLIENT_PREFACE[..first.len()] || second != &CLIENT_PREFACE[first.len()..] {
            return Err(ConnError::BadPreface);
        }
        self.ensure_event_capacity()?;
        self.bytes.consume(CLIENT_PREFACE.len());
        self.preface_done = true;
        self.push_event(Event::PrefaceComplete)?;
        Ok(true)
    }

    pub(super) fn complete_preface(&mut self) {
        debug_assert!(self.preface_done);
        self.events
            .vacant_entry()
            .unwrap()
            .push_back(Event::PrefaceComplete);
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
                    self.bytes.consume(total);
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if header.length > max_frame_size {
                return Err(ConnError::FrameSize);
            }
            let total = HEADER_LEN + header.length as usize;
            return Ok((self.bytes.len() >= total).then_some(header));
        }
    }

    fn parse_frame_header(&self) -> Result<FrameHeader, ParseError> {
        let Some((first, second)) = self.bytes.range_slices(0, HEADER_LEN) else {
            return Err(ParseError::NeedMore);
        };
        if second.is_empty() {
            return FrameHeader::parse(first);
        }
        let mut bytes = [0; HEADER_LEN];
        bytes[..first.len()].copy_from_slice(first);
        bytes[first.len()..].copy_from_slice(second);
        FrameHeader::parse(&bytes)
    }

    pub(super) fn unpadded_payload(
        &self,
        header: FrameHeader,
    ) -> Result<(usize, usize), ParseError> {
        let mut start = HEADER_LEN;
        let mut len = header.length as usize;
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

    pub(super) fn data(&mut self, start: usize, len: usize) -> Result<Pooled, ConnError> {
        let mut lease = self.data_pool.try_acquire().ok_or(ConnError::Overload)?;
        let (first, second) = self
            .bytes
            .range_slices(start, len)
            .ok_or(ConnError::FrameSize)?;
        let mut writer = lease.spare_writer();
        writer
            .try_extend_from_slice(first)
            .map_err(|_| ConnError::Overload)?;
        writer
            .try_extend_from_slice(second)
            .map_err(|_| ConnError::Overload)?;
        drop(writer);
        Ok(lease.freeze())
    }

    pub(super) fn consume(&mut self, n: usize) {
        self.bytes.consume(n);
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

    pub(super) fn clear_headers(&mut self) {
        self.header_block.clear();
    }

    pub(super) fn header_remaining(&self) -> usize {
        self.header_cap.saturating_sub(self.header_block.len())
    }

    pub(super) fn extend_headers(&mut self, start: usize, len: usize) -> Result<(), ConnError> {
        if len > self.header_remaining() {
            return Err(ConnError::HeaderListTooLarge);
        }
        let (first, second) = self
            .bytes
            .range_slices(start, len)
            .ok_or(ConnError::FrameSize)?;
        self.header_block.extend_from_slice(first);
        self.header_block.extend_from_slice(second);
        Ok(())
    }

    pub(super) fn decode_headers(&mut self) -> Result<(hpack::HeaderBlock, bool), ConnError> {
        let mut lease = self.header_pool.try_acquire().ok_or(ConnError::Overload)?;
        let mut block = core::mem::take(&mut self.header_block);
        let mut overflow = false;
        let decoded = self.decoder.decode_bounded(&block, |name, value| {
            if overflow {
                return;
            }
            let Ok(name_len) = u32::try_from(name.len()) else {
                overflow = true;
                return;
            };
            let Ok(value_len) = u32::try_from(value.len()) else {
                overflow = true;
                return;
            };
            let mut writer = lease.spare_writer();
            overflow = writer
                .try_extend_from_slice(&name_len.to_ne_bytes())
                .and_then(|()| writer.try_extend_from_slice(&value_len.to_ne_bytes()))
                .and_then(|()| writer.try_extend_from_slice(name))
                .and_then(|()| writer.try_extend_from_slice(value))
                .is_err();
        });
        block.clear();
        self.header_block = block;
        let over_limit = decoded?;
        if overflow {
            return Err(ConnError::Overload);
        }
        Ok((hpack::HeaderBlock::from_pooled(lease.freeze()), over_limit))
    }

    pub(super) fn has_pending_headers(&self) -> bool {
        self.pending_headers.is_some()
    }

    pub(super) fn start_headers(&mut self, pending: PendingHeaders) {
        self.pending_headers = Some(pending);
    }

    pub(super) fn pending_headers_mut(&mut self) -> Option<&mut PendingHeaders> {
        self.pending_headers.as_mut()
    }

    pub(super) fn take_pending_headers(&mut self) -> Option<PendingHeaders> {
        self.pending_headers.take()
    }
}
