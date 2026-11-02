use o3::buffer::{self, segments, storage, storage::inline};

use crate::message::{Compression, Error, Frame, HEADER_LEN, Payload, Wire, input};

/// A complete header whose compression and length passed admission once.
#[derive(Clone, Copy)]
struct Header {
    len: u32,
    compression: Compression,
}

#[derive(Clone, Copy)]
enum Failure {
    Compression(u8),
    Length(u32),
}

impl Failure {
    fn error(self, max: usize) -> Error {
        match self {
            Self::Compression(flag) => Error::BadCompressionFlag(flag),
            Self::Length(len) => Error::MessageTooLarge {
                len: len as usize,
                max,
            },
        }
    }
}

impl Header {
    fn try_new(bytes: &[u8; HEADER_LEN], max: usize) -> Result<Self, Failure> {
        let [flag, length @ ..] = bytes;
        let mode = match flag {
            0 => Compression::Plain,
            1 => Compression::Compressed,
            other => return Err(Failure::Compression(*other)),
        };
        let len = u32::from_be_bytes(*length);
        if len as usize > max {
            return Err(Failure::Length(len));
        }
        Ok(Self {
            len,
            compression: mode,
        })
    }
}

enum State {
    Header(inline::Bytes<HEADER_LEN>),
    Body(Header),
    Failed(Failure),
}

pub struct Deframer<C: buffer::raw::Segment = storage::Shared> {
    max_message_len: usize,
    state: State,
    body: Option<Payload<C>>,
}

impl<C: buffer::raw::Segment> Deframer<C> {
    pub fn new(max: usize) -> Self {
        Self {
            max_message_len: max,
            state: State::Header(inline::Bytes::new()),
            body: None,
        }
    }

    pub fn is_idle(&self) -> bool {
        matches!(&self.state, State::Header(header) if header.is_empty())
    }

    pub(crate) fn count(&self) -> segments::Count {
        self.body
            .as_ref()
            .map_or(segments::Count::EMPTY, Payload::count)
    }

    /// A rejected header remains rejected without consuming more input.
    /// Storage failures retain the admitted header so the operation can retry.
    /// `Ok(None)` exhausts the input.
    pub fn next<I: input::Source<C>>(
        &mut self,
        input: &mut I,
        pool: &segments::Pool<C>,
    ) -> Result<Option<Frame<C, Wire>>, Error> {
        let header = match &mut self.state {
            State::Header(bytes) => {
                if input.is_empty() {
                    return Ok(None);
                }
                bytes
                    .fit(input.len())
                    .try_write(|output| input.try_copy(output))
                    .map_err(Error::Input)?;
                let Some(bytes) = bytes.as_ref().first_chunk::<HEADER_LEN>() else {
                    return Ok(None);
                };
                let header = match Header::try_new(bytes, self.max_message_len) {
                    Ok(header) => header,
                    Err(failure) => {
                        self.state = State::Failed(failure);
                        return Err(failure.error(self.max_message_len));
                    }
                };
                self.state = State::Body(header);
                header
            }
            State::Body(header) => *header,
            State::Failed(failure) => return Err(failure.error(self.max_message_len)),
        };
        let needed = header.len as usize;
        if needed == 0 {
            let payload = Payload::from(pool.try_chain().map_err(Error::Retain)?);
            return Ok(Some(self.finish(header, payload)));
        }
        if !input.is_empty() {
            if self.body.is_none() && input.len() >= needed {
                let mut payload = Payload::from(pool.try_chain().map_err(Error::Retain)?);
                input
                    .try_move(needed, &mut payload)
                    .map_err(Error::Transfer)?;
                return Ok(Some(self.finish(header, payload)));
            }

            let already = self.body.as_ref().map_or(0, |payload| payload.len());
            let left = needed - already;
            let n = left.min(input.len());
            match &mut self.body {
                Some(body) => {
                    input.try_move(n, body).map_err(Error::Transfer)?;
                }
                None => {
                    let mut body = Payload::from(pool.try_chain().map_err(Error::Retain)?);
                    input.try_move(n, &mut body).map_err(Error::Transfer)?;
                    self.body = Some(body);
                }
            }
            if let Some(payload) = self.body.take_if(|body| body.len() == needed) {
                return Ok(Some(self.finish(header, payload)));
            }
        }
        Ok(None)
    }

    fn finish(&mut self, header: Header, payload: Payload<C>) -> Frame<C, Wire> {
        self.state = State::Header(inline::Bytes::new());
        Frame {
            payload,
            state: Wire {
                compression: header.compression,
            },
        }
    }
}

const _: () = assert!(
    size_of::<Deframer>()
        <= size_of::<(
            usize,
            inline::Bytes<HEADER_LEN>,
            Option<Payload>,
            usize,
            Compression,
        )>()
);
