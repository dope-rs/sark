use std::collections::VecDeque;
use std::num::NonZeroUsize;

use o3::buffer::{ByteRing, ByteSink, CapacityError, Shared};

use crate::conn::{ConnError, Settings};
use crate::frame::{
    self, Continuation, ErrorCode, Flags, FrameHeader, HEADER_LEN, Headers, RstStream,
    WindowIncrement, WindowUpdate,
};
use crate::hpack;
use crate::stream::StreamId;

struct SharedPayload {
    prefix_len: usize,
    body: Shared,
}

pub(crate) enum Write {
    Buffered(usize),
    Split { prefix_len: usize, body: Shared },
}

pub(crate) struct Egress {
    bytes: ByteRing,
    capacity: usize,
    encoder: hpack::Encoder,
    header_block: Vec<u8>,
    payloads: VecDeque<SharedPayload>,
    payload_bytes: usize,
    unassigned_prefix: usize,
}

impl Egress {
    pub(crate) fn from_capacity(
        capacity: NonZeroUsize,
        table_size: usize,
        header_capacity: usize,
    ) -> Self {
        Self {
            bytes: ByteRing::with_capacity(capacity),
            capacity: capacity.get(),
            encoder: hpack::Encoder::new(table_size),
            header_block: Vec::with_capacity(header_capacity),
            payloads: VecDeque::new(),
            payload_bytes: 0,
            unassigned_prefix: 0,
        }
    }

    pub(crate) fn raw_mut(&mut self) -> &mut Self {
        self
    }

    pub(crate) fn first(&self) -> &[u8] {
        let (first, _) = self.slices();
        first
    }

    pub(crate) fn slices(&self) -> (&[u8], &[u8]) {
        let limit = self
            .payloads
            .front()
            .map_or(self.bytes.len(), |payload| payload.prefix_len);
        let (first, second) = self.bytes.as_slices();
        let first_len = first.len().min(limit);
        let second_len = second.len().min(limit - first_len);
        (&first[..first_len], &second[..second_len])
    }

    pub(crate) fn drain(&mut self, bytes: usize) {
        let available = self
            .payloads
            .front()
            .map_or(self.bytes.len(), |payload| payload.prefix_len);
        let consumed = bytes.min(available);
        self.bytes.consume_prefix_up_to(consumed);
        if let Some(payload) = self.payloads.front_mut() {
            payload.prefix_len -= consumed;
        } else {
            self.unassigned_prefix -= consumed;
        }
    }

    pub(crate) fn drain_into(&mut self, write: &mut [u8]) -> usize {
        let (first, second) = self.slices();
        let first_len = first.len().min(write.len());
        write[..first_len].copy_from_slice(&first[..first_len]);
        let second_len = second.len().min(write.len() - first_len);
        write[first_len..first_len + second_len].copy_from_slice(&second[..second_len]);
        let written = first_len + second_len;
        self.drain(written);
        written
    }

    pub(crate) fn take_write(&mut self, write: &mut [u8]) -> Write {
        let Some(prefix_len) = self.payloads.front().map(|payload| payload.prefix_len) else {
            return Write::Buffered(self.drain_into(write));
        };
        let capacity = write.len().min(prefix_len);
        let written = self.drain_into(&mut write[..capacity]);
        if written != prefix_len {
            return Write::Buffered(written);
        }
        let Some(payload) = self.payloads.pop_front() else {
            return Write::Buffered(written);
        };
        self.payload_bytes -= payload.body.len();
        Write::Split {
            prefix_len: written,
            body: payload.body,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len() + self.payload_bytes
    }

    pub(crate) fn reserve(&self, additional: usize) -> Result<(), ConnError> {
        if additional > self.bytes.remaining()
            || additional > self.capacity.saturating_sub(self.len())
        {
            Err(ConnError::Overload)
        } else {
            Ok(())
        }
    }

    pub(crate) fn reserve_split(
        &self,
        prefix_len: usize,
        payload_len: usize,
    ) -> Result<(), ConnError> {
        let additional = prefix_len
            .checked_add(payload_len)
            .ok_or(ConnError::FrameSize)?;
        if prefix_len > self.bytes.remaining()
            || additional > self.capacity.saturating_sub(self.len())
        {
            Err(ConnError::Overload)
        } else {
            Ok(())
        }
    }

    pub(crate) fn over_capacity(&self) -> bool {
        self.len() > self.capacity
    }

    pub(crate) fn queue_shared(&mut self, body: Shared) {
        if body.is_empty() {
            return;
        }
        self.payload_bytes += body.len();
        self.payloads.push_back(SharedPayload {
            prefix_len: std::mem::take(&mut self.unassigned_prefix),
            body,
        });
    }

    pub(crate) fn try_extend_from_slice(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), o3::buffer::CapacityError> {
        self.bytes.try_extend_from_slice(bytes)?;
        self.unassigned_prefix += bytes.len();
        Ok(())
    }

    pub(crate) fn initial_settings(&mut self, settings: &Settings) -> Result<(), ConnError> {
        let count = settings.param_count();
        let header = FrameHeader {
            length: frame::FrameLength::from_usize(count * 6).ok_or(ConnError::FrameSize)?,
            kind: frame::Type::Settings,
            flags: Flags(0),
            stream_id: StreamId::CONNECTION,
        }
        .wire_bytes();
        let (params, len) = settings.wire_bytes();
        self.write_slices([&header, &params[..len]])?;
        Ok(())
    }

    pub(crate) fn settings_ack(&mut self) -> Result<(), ConnError> {
        self.reserve(HEADER_LEN)?;
        FrameHeader {
            length: frame::FrameLength::ZERO,
            kind: frame::Type::Settings,
            flags: Flags(Flags::ACK),
            stream_id: StreamId::CONNECTION,
        }
        .encode(self)?;
        Ok(())
    }

    pub(crate) fn window_update(
        &mut self,
        stream_id: StreamId,
        increment: WindowIncrement,
    ) -> Result<(), ConnError> {
        self.reserve(HEADER_LEN + 4)?;
        WindowUpdate {
            stream_id,
            increment,
        }
        .encode(self)?;
        Ok(())
    }

    pub(crate) fn reset(&mut self, stream_id: StreamId, error: ErrorCode) -> Result<(), ConnError> {
        self.reserve(HEADER_LEN + 4)?;
        RstStream::new(stream_id, error)
            .ok_or(ConnError::Protocol)?
            .encode(self)?;
        Ok(())
    }

    pub(crate) fn set_header_table_size(&mut self, size: usize) {
        self.encoder.set_max_size(size);
    }

    pub(crate) fn headers<'a, I>(
        &mut self,
        stream_id: StreamId,
        headers: I,
        end_stream: bool,
        max_frame: usize,
    ) -> Result<(), ConnError>
    where
        I: IntoIterator<Item = hpack::Header<'a>>,
    {
        let mut block = core::mem::take(&mut self.header_block);
        block.clear();
        self.encoder.encode(headers, &mut block);
        let frames = block.len().max(1).div_ceil(max_frame);
        let additional = frames
            .checked_mul(HEADER_LEN)
            .and_then(|heads| block.len().checked_add(heads))
            .ok_or(ConnError::FrameSize);
        let result = additional.and_then(|additional| self.reserve(additional));
        if result.is_err() {
            self.header_block = block;
            return result;
        }
        if block.len() <= max_frame {
            Headers::new(stream_id, end_stream, true, None, &block)
                .ok_or(ConnError::FrameSize)?
                .encode(self)?;
        } else {
            let (first, rest) = block.split_at(max_frame);
            Headers::new(stream_id, end_stream, false, None, first)
                .ok_or(ConnError::FrameSize)?
                .encode(self)?;
            let mut position = 0;
            while position < rest.len() {
                let take = (rest.len() - position).min(max_frame);
                let end = position + take;
                Continuation::new(stream_id, end == rest.len(), &rest[position..end])
                    .ok_or(ConnError::FrameSize)?
                    .encode(self)?;
                position = end;
            }
        }
        self.header_block = block;
        Ok(())
    }
}

impl ByteSink for Egress {
    type Error = CapacityError;

    fn write_slices<const N: usize>(&mut self, slices: [&[u8]; N]) -> Result<(), Self::Error> {
        let before = self.bytes.len();
        self.bytes.try_extend_from_slices(slices)?;
        self.unassigned_prefix += self.bytes.len() - before;
        Ok(())
    }
}
