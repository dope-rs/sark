use o3::buffer::{Bytes, Pooled, Retained, SharedLease, SharedPool};
use sark_h2::conn::DataPayload;

pub struct MessageFrame {
    pub compressed: bool,
    pub payload: Bytes<Retained>,
}

impl std::fmt::Debug for MessageFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageFrame")
            .field("compressed", &self.compressed)
            .field("len", &self.payload.len())
            .finish()
    }
}

impl PartialEq for MessageFrame {
    fn eq(&self, other: &Self) -> bool {
        self.compressed == other.compressed && self.payload.as_slice() == other.payload.as_slice()
    }
}

impl Eq for MessageFrame {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameError {
    BadCompressionFlag(u8),
    MessageTooLarge { len: usize, max: usize },
    LengthOverflow,
    Capacity,
}

impl MessageFrame {
    pub fn header(compressed: bool, len: usize) -> Result<[u8; 5], FrameError> {
        let len = u32::try_from(len).map_err(|_| FrameError::LengthOverflow)?;
        let bytes = len.to_be_bytes();
        Ok([u8::from(compressed), bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    pub fn encode(compressed: bool, payload: &[u8], out: &mut Vec<u8>) -> Result<(), FrameError> {
        out.extend_from_slice(&Self::header(compressed, payload.len())?);
        out.extend_from_slice(payload);
        Ok(())
    }
}

pub(crate) struct DataChunk {
    pooled: Option<Pooled>,
    len: usize,
    pos: usize,
}

impl DataChunk {
    pub(crate) fn new(data: DataPayload) -> Self {
        Self::from_pooled(data.into_pooled())
    }

    pub(crate) fn from_pooled(pooled: Pooled) -> Self {
        let len = pooled.len();
        Self {
            pooled: Some(pooled),
            len,
            pos: 0,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pos == self.len
    }

    fn remaining(&self) -> &[u8] {
        &self.pooled.as_ref().unwrap().as_slice()[self.pos..]
    }

    fn advance(&mut self, len: usize) {
        self.pos += len;
    }

    fn take_retained(&mut self, len: usize) -> Bytes<Retained> {
        let start = self.pos;
        let end = start + len;
        let pooled = if end == self.len {
            self.pooled.take().unwrap()
        } else {
            self.pooled.as_ref().unwrap().clone()
        };
        self.pos = end;
        Bytes::<Retained>::from(pooled).slice(start..end)
    }
}

pub struct Deframer {
    max_message_len: usize,
    header: [u8; 5],
    header_len: usize,
    body: Option<SharedLease>,
    needed: usize,
    compressed: bool,
}

impl Deframer {
    pub fn new(max_message_len: usize) -> Self {
        Self {
            max_message_len,
            header: [0; 5],
            header_len: 0,
            body: None,
            needed: 0,
            compressed: false,
        }
    }

    pub(crate) fn next(
        &mut self,
        input: &mut DataChunk,
        pool: &SharedPool,
    ) -> Result<Option<MessageFrame>, FrameError> {
        while !input.is_empty() {
            if self.header_len < self.header.len() {
                let n = (self.header.len() - self.header_len).min(input.remaining().len());
                self.header[self.header_len..self.header_len + n]
                    .copy_from_slice(&input.remaining()[..n]);
                self.header_len += n;
                input.advance(n);
                if self.header_len < self.header.len() {
                    return Ok(None);
                }
                self.begin_body()?;
                if self.needed == 0 {
                    let payload = input.take_retained(0);
                    let message = MessageFrame {
                        compressed: self.compressed,
                        payload,
                    };
                    self.reset_header();
                    return Ok(Some(message));
                }
            }

            if self.body.is_none() && input.remaining().len() >= self.needed {
                let payload = input.take_retained(self.needed);
                let message = MessageFrame {
                    compressed: self.compressed,
                    payload,
                };
                self.reset_header();
                return Ok(Some(message));
            }

            let body = match &mut self.body {
                Some(body) => body,
                None => self
                    .body
                    .insert(pool.try_acquire().ok_or(FrameError::Capacity)?),
            };
            let n = (self.needed - body.len()).min(input.remaining().len());
            body.spare_writer()
                .try_extend_from_slice(&input.remaining()[..n])
                .map_err(|_| FrameError::Capacity)?;
            input.advance(n);
            if body.len() == self.needed {
                let body = self.body.take().unwrap().freeze();
                let message = MessageFrame {
                    compressed: self.compressed,
                    payload: Bytes::<Retained>::from(body),
                };
                self.reset_header();
                return Ok(Some(message));
            }
        }
        Ok(None)
    }

    fn begin_body(&mut self) -> Result<(), FrameError> {
        self.compressed = match self.header[0] {
            0 => false,
            1 => true,
            other => return Err(FrameError::BadCompressionFlag(other)),
        };
        let len = u32::from_be_bytes([
            self.header[1],
            self.header[2],
            self.header[3],
            self.header[4],
        ]) as usize;
        if len > self.max_message_len {
            return Err(FrameError::MessageTooLarge {
                len,
                max: self.max_message_len,
            });
        }
        self.needed = len;
        Ok(())
    }

    fn reset_header(&mut self) {
        self.header_len = 0;
        self.needed = 0;
        self.compressed = false;
    }
}
