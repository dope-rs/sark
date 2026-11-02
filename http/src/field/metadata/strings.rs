//! HPACK and QPACK string literal decoding.

use o3::buffer::write;

use crate::field::metadata::huffman::decoder;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Plain,
    Huffman,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    Pending,
    Complete,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Plain,
    Huffman(decoder::Decoder),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    remaining: usize,
    decoded_len: u32,
    state: State,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    consumed: usize,
    decoded: usize,
}

impl Cursor {
    pub const fn new(len: usize, encoding: Encoding) -> Self {
        Self {
            remaining: len,
            decoded_len: 0,
            state: match encoding {
                Encoding::Plain => State::Plain,
                Encoding::Huffman => State::Huffman(decoder::Decoder::new()),
            },
        }
    }

    pub const fn remaining(&self) -> usize {
        self.remaining
    }

    pub const fn decoded_len(&self) -> usize {
        self.decoded_len as usize
    }

    pub const fn completion(&self) -> Completion {
        if self.remaining == 0 {
            Completion::Complete
        } else {
            Completion::Pending
        }
    }

    pub fn output_bound(&self, input: usize) -> Result<usize, decoder::Error> {
        let consumed = input.min(self.remaining);
        match self.state {
            State::Huffman(_) => consumed.checked_mul(2).ok_or(decoder::Error),
            State::Plain => Ok(consumed),
        }
    }

    pub fn feed_count(&mut self, input: &[u8]) -> Result<Progress, decoder::Error> {
        let consumed = input.len().min(self.remaining);
        let payload = &input[..consumed];
        let (state, decoded) = match self.state {
            State::Huffman(decoder) => {
                let (next, decoded) = decoder.feed_count(payload)?;
                (State::Huffman(next), decoded)
            }
            State::Plain => (State::Plain, consumed),
        };
        self.advance(consumed, decoded, state)
    }

    pub fn feed_into<W: write::Sink>(
        &mut self,
        input: &[u8],
        sink: W,
    ) -> Result<(Progress, W), decoder::Failure<W, W::Error>> {
        let consumed = input.len().min(self.remaining);
        let payload = &input[..consumed];
        let (state, sink, decoded) = match self.state {
            State::Huffman(decoder) => {
                let (next, sink, decoded) = decoder.feed_into(payload, sink)?.into_parts();
                (State::Huffman(next), sink, decoded)
            }
            State::Plain => {
                let mut sink = sink;
                sink = match write::Sink::write_slice(&mut sink, payload) {
                    Ok(()) => Ok(sink),
                    Err(cause) => Err(decoder::Failure {
                        sink,
                        cause: decoder::DecodeError::Sink(cause),
                        progress: decoder::Progress::new(0, 0),
                    }),
                }?;
                (State::Plain, sink, consumed)
            }
        };
        let progress = match self.advance(consumed, decoded, state) {
            Ok(progress) => progress,
            Err(cause) => {
                return Err(decoder::Failure {
                    sink,
                    cause: decoder::DecodeError::InvalidEncoding(cause),
                    progress: decoder::Progress::new(consumed, decoded),
                });
            }
        };
        Ok((progress, sink))
    }

    pub fn finish(self) -> Result<usize, decoder::Error> {
        if self.remaining != 0 {
            return Err(decoder::Error);
        }
        match self.state {
            State::Huffman(decoder) => decoder.finish()?,
            State::Plain => {}
        }
        Ok(self.decoded_len())
    }
    fn advance(
        &mut self,
        consumed: usize,
        decoded: usize,
        state: State,
    ) -> Result<Progress, decoder::Error> {
        if decoded > u32::MAX as usize {
            return Err(decoder::Error);
        }
        let decoded = decoded as u32;
        let total = self
            .decoded_len
            .checked_add(decoded)
            .ok_or(decoder::Error)?;
        self.remaining -= consumed;
        self.decoded_len = total;
        self.state = state;
        Ok(Progress {
            consumed,
            decoded: decoded as usize,
        })
    }
}

impl Progress {
    pub const fn consumed(self) -> usize {
        self.consumed
    }

    pub const fn decoded(self) -> usize {
        self.decoded
    }
}

const _: () = {
    assert!(core::mem::size_of::<Encoding>() == core::mem::size_of::<bool>());
    assert!(core::mem::size_of::<Completion>() == core::mem::size_of::<bool>());
    assert!(core::mem::size_of::<State>() == core::mem::size_of::<Option<decoder::Decoder>>());
    assert!(core::mem::align_of::<State>() == core::mem::align_of::<Option<decoder::Decoder>>());
};
