use o3::buffer::ByteRing;

use crate::conn::{ConnError, Settings};
use crate::frame::{
    self, Continuation, ErrorCode, Flags, FrameHeader, HEADER_LEN, Headers, RstStream, WindowUpdate,
};
use crate::hpack;
use crate::stream::StreamId;

pub(crate) struct Egress {
    bytes: ByteRing,
    capacity: usize,
    encoder: hpack::Encoder,
    header_block: Vec<u8>,
}

impl Egress {
    pub(crate) fn new(capacity: usize, table_size: usize, header_capacity: usize) -> Self {
        Self {
            bytes: ByteRing::with_capacity(capacity),
            capacity,
            encoder: hpack::Encoder::new(table_size),
            header_block: Vec::with_capacity(header_capacity),
        }
    }

    pub(crate) fn raw_mut(&mut self) -> &mut ByteRing {
        &mut self.bytes
    }

    pub(crate) fn first(&self) -> &[u8] {
        self.bytes.as_slices().0
    }

    pub(crate) fn slices(&self) -> (&[u8], &[u8]) {
        self.bytes.as_slices()
    }

    pub(crate) fn drain(&mut self, bytes: usize) {
        self.bytes.consume(bytes.min(self.bytes.len()));
    }

    pub(crate) fn drain_into(&mut self, write: &mut [u8]) -> usize {
        let (first, second) = self.bytes.as_slices();
        let first_len = first.len().min(write.len());
        write[..first_len].copy_from_slice(&first[..first_len]);
        let second_len = second.len().min(write.len() - first_len);
        write[first_len..first_len + second_len].copy_from_slice(&second[..second_len]);
        let written = first_len + second_len;
        self.drain(written);
        written
    }

    pub(crate) fn reserve(&self, additional: usize) -> Result<(), ConnError> {
        if additional > self.bytes.remaining() {
            Err(ConnError::Overload)
        } else {
            Ok(())
        }
    }

    pub(crate) fn over_capacity(&self) -> bool {
        self.bytes.len() > self.capacity
    }

    pub(crate) fn initial_settings(&mut self, settings: &Settings) {
        let count = settings.param_count();
        FrameHeader {
            length: (count * 6) as u32,
            kind: frame::Type::Settings,
            flags: Flags(0),
            stream_id: StreamId::CONNECTION,
        }
        .encode(&mut self.bytes);
        settings.encode(&mut self.bytes);
    }

    pub(crate) fn settings_ack(&mut self) -> Result<(), ConnError> {
        self.reserve(HEADER_LEN)?;
        FrameHeader {
            length: 0,
            kind: frame::Type::Settings,
            flags: Flags(Flags::ACK),
            stream_id: StreamId::CONNECTION,
        }
        .encode(&mut self.bytes);
        Ok(())
    }

    pub(crate) fn window_update(
        &mut self,
        stream_id: StreamId,
        increment: u32,
    ) -> Result<(), ConnError> {
        if increment == 0 {
            return Ok(());
        }
        self.reserve(HEADER_LEN + 4)?;
        WindowUpdate {
            stream_id,
            increment,
        }
        .encode(&mut self.bytes);
        Ok(())
    }

    pub(crate) fn reset(&mut self, stream_id: StreamId, error: ErrorCode) -> Result<(), ConnError> {
        self.reserve(HEADER_LEN + 4)?;
        RstStream { stream_id, error }.encode(&mut self.bytes);
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
            Headers {
                stream_id,
                end_stream,
                end_headers: true,
                priority: None,
                block_fragment: &block,
            }
            .encode(&mut self.bytes);
        } else {
            let (first, rest) = block.split_at(max_frame);
            Headers {
                stream_id,
                end_stream,
                end_headers: false,
                priority: None,
                block_fragment: first,
            }
            .encode(&mut self.bytes);
            let mut position = 0;
            while position < rest.len() {
                let take = (rest.len() - position).min(max_frame);
                let end = position + take;
                Continuation {
                    stream_id,
                    end_headers: end == rest.len(),
                    block_fragment: &rest[position..end],
                }
                .encode(&mut self.bytes);
                position = end;
            }
        }
        self.header_block = block;
        Ok(())
    }
}
