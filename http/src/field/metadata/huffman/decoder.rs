use o3::buffer::write::Sink;

use crate::field::metadata::huffman::{BYTE_DECODE_MASK, BYTE_ERROR, BYTE_MAYBE_EOS, BYTE_TABLE};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Error;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DecodeError<E> {
    InvalidEncoding(Error),
    Sink(E),
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Progress {
    consumed: usize,
    decoded: usize,
}

pub struct Failure<W, E> {
    pub sink: W,
    pub cause: DecodeError<E>,
    pub progress: Progress,
}

#[must_use = "consume the complete Huffman output"]
pub struct Output<W> {
    decoder: Decoder,
    sink: W,
    decoded: usize,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Tail {
    Invalid,
    Valid,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Decoder {
    state: u8,
    tail: Tail,
}

impl Tail {
    const fn from_flags(flags: u8) -> Self {
        match flags & BYTE_MAYBE_EOS {
            0 => Self::Invalid,
            _ => Self::Valid,
        }
    }
}

impl Decoder {
    pub const fn new() -> Self {
        Self {
            state: 0,
            tail: Tail::Valid,
        }
    }

    pub fn feed<E, F>(self, input: &[u8], mut sink: F) -> Result<Output<F>, Failure<F, E>>
    where
        F: FnMut(u8) -> Result<(), E>,
    {
        match self.transduce(input, &mut sink) {
            Ok((decoder, progress)) => Ok(Output::new(decoder, sink, progress.decoded)),
            Err((cause, progress)) => Err(Failure {
                sink,
                cause,
                progress,
            }),
        }
    }

    pub fn feed_count(mut self, input: &[u8]) -> Result<(Self, usize), Error> {
        let mut state = self.state as usize;
        let mut tail = self.tail;
        let mut decoded = 0usize;
        for &byte in input {
            let entry = BYTE_TABLE[state][byte as usize];
            if entry.flags & BYTE_ERROR != 0 {
                return Err(Error);
            }
            decoded += usize::from(entry.flags & BYTE_DECODE_MASK);
            state = entry.next_state as usize;
            tail = Tail::from_flags(entry.flags);
        }
        self.state = state as u8;
        self.tail = tail;
        Ok((self, decoded))
    }

    pub fn feed_into<W: Sink>(
        self,
        input: &[u8],
        mut sink: W,
    ) -> Result<Output<W>, Failure<W, W::Error>> {
        let result = self.transduce(input, |byte| sink.write_byte(byte));
        let (next, progress) = match result {
            Ok(output) => output,
            Err((cause, progress)) => {
                return Err(Failure {
                    sink,
                    cause,
                    progress,
                });
            }
        };
        Ok(Output::new(next, sink, progress.decoded))
    }

    fn transduce<E>(
        mut self,
        input: &[u8],
        mut emit: impl FnMut(u8) -> Result<(), E>,
    ) -> Result<(Self, Progress), (DecodeError<E>, Progress)> {
        let mut state = self.state as usize;
        let mut tail = self.tail;
        let mut progress = Progress {
            consumed: 0,
            decoded: 0,
        };
        for &byte in input {
            let entry = BYTE_TABLE[state][byte as usize];
            if entry.flags & BYTE_ERROR != 0 {
                return Err((DecodeError::InvalidEncoding(Error), progress));
            }
            let count = entry.flags & BYTE_DECODE_MASK;
            if count != 0 {
                emit(entry.first).map_err(|cause| (DecodeError::Sink(cause), progress))?;
                progress.decoded += 1;
            }
            if count == 2 {
                emit(entry.second).map_err(|cause| (DecodeError::Sink(cause), progress))?;
                progress.decoded += 1;
            }
            state = entry.next_state as usize;
            tail = Tail::from_flags(entry.flags);
            progress.consumed += 1;
        }
        self.state = state as u8;
        self.tail = tail;
        Ok((self, progress))
    }

    pub fn finish(self) -> Result<(), Error> {
        match self.tail {
            Tail::Valid => Ok(()),
            Tail::Invalid => Err(Error),
        }
    }
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

impl<W> Output<W> {
    fn new(decoder: Decoder, sink: W, decoded: usize) -> Self {
        Self {
            decoder,
            sink,
            decoded,
        }
    }

    pub fn into_parts(self) -> (Decoder, W, usize) {
        (self.decoder, self.sink, self.decoded)
    }
}

impl Progress {
    pub(crate) const fn new(consumed: usize, decoded: usize) -> Self {
        Self { consumed, decoded }
    }

    pub const fn consumed(self) -> usize {
        self.consumed
    }

    pub const fn decoded(self) -> usize {
        self.decoded
    }
}

const _: () = {
    assert!(core::mem::size_of::<Tail>() == core::mem::size_of::<bool>());
    assert!(core::mem::size_of::<Decoder>() == core::mem::size_of::<[u8; 2]>());
    assert!(core::mem::align_of::<Decoder>() == core::mem::align_of::<u8>());
};
