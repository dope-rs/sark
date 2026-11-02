use std::num;

use chunk::{scan, trailer::Partial};
use o3::{alloc, buffer::write::Sink as _};

use crate::{
    h1::{
        chunk,
        codec::decode::{fields, fields::Sink as _, trailer},
    },
    payload::response::headers::List,
};

const MAX_BODY_SIZE: usize = 100 * 1024 * 1024;

#[derive(Debug)]
pub enum Error {
    Framing(chunk::Error),
    Field(Conversion),
    Allocation(alloc::Error),
    Incomplete,
}

#[derive(Debug)]
pub struct Conversion {
    pub field: usize,
    pub cause: fields::Error,
}

pub(crate) struct Output<'field> {
    pub(crate) body: Vec<u8>,
    pub(crate) trailers: List<'field>,
}

impl<'field> Output<'field> {
    pub(crate) fn drain(buf: &[u8], max_body: usize) -> Result<Option<Self>, Error> {
        let mut decoder = Decoder::with_limit(max_body);
        let mut body = Vec::new();
        let mut input = buf;

        loop {
            let plan = decoder.decode(input)?;
            match plan.commit(&mut decoder) {
                Event::NeedMore(_) => return Ok(None),
                Event::Chunk {
                    bytes, consumed, ..
                } => {
                    // SAFETY: decode just admitted this consumed prefix on
                    // this unchanged input; the event retains that exact borrow.
                    input = unsafe { input.get_unchecked(consumed.get()..) };
                    body.write_slice(bytes).map_err(Error::Allocation)?;
                }
                Event::Done { fields, .. } => {
                    let trailers = Decoder::parse(fields).map_err(Error::Field)?;
                    return Ok(Some(Self { body, trailers }));
                }
            }
        }
    }
}

pub enum Event<'a> {
    NeedMore(Option<num::NonZero<usize>>),
    Chunk {
        consumed: num::NonZero<usize>,
        bytes: &'a [u8],
        boundary: chunk::Boundary,
    },
    Done {
        consumed: num::NonZero<usize>,
        fields: trailer::Fields<'a>,
    },
}

pub struct Plan<'a> {
    event: Prepared<'a>,
}

enum Prepared<'a> {
    Event { next: Decoder, event: Event<'a> },
    Trailer(Partial<'a>),
}

impl<'a> Plan<'a> {
    pub fn commit(self, decoder: &mut Decoder) -> Event<'a> {
        match self.event {
            Prepared::Event { next, event } => {
                *decoder = next;
                event
            }
            Prepared::Trailer(_) => Event::NeedMore(None),
        }
    }

    /// Transfers partial trailer admission together with its exact input borrow.
    /// Retaining that prefix permits later input to be scanned only once.
    pub fn into_trailer(self) -> Result<trailer::Pending<'a>, Self> {
        match self.event {
            Prepared::Trailer(partial) => Ok(trailer::Pending::new(partial)),
            event @ Prepared::Event { .. } => Err(Self { event }),
        }
    }
}

#[derive(Clone, Copy)]
pub struct Decoder {
    framer: chunk::Framer,
    limits: chunk::Limit,
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder {
    pub const DEFAULT_MAX_BODY: usize = MAX_BODY_SIZE;

    pub fn new() -> Self {
        Self::with_limit(MAX_BODY_SIZE)
    }

    pub fn with_limit(max_body: usize) -> Self {
        Self {
            framer: chunk::Framer::new(),
            limits: chunk::Limit::with_body(max_body),
        }
    }

    /// Bounds an emitted body fragment without copying or limiting framing
    /// progress.
    pub fn decode_max<'a>(&self, buf: &'a [u8], max: usize) -> Result<Plan<'a>, Error> {
        let (framer, event) = self
            .framer
            .prepare::<scan::Fragments>(buf, &self.limits, max)
            .map_err(Error::Framing)?;
        let event = match event {
            scan::Decoded::Data {
                consumed,
                bytes,
                boundary,
            } => Event::Chunk {
                consumed,
                bytes,
                boundary,
            },
            scan::Decoded::Trailer(partial) => {
                return Ok(Plan {
                    event: Prepared::Trailer(partial),
                });
            }
            scan::Decoded::Complete {
                consumed,
                bytes: lines,
            } => Event::Done {
                consumed,
                fields: trailer::Fields::new(lines),
            },
            scan::Decoded::More(consumed) => Event::NeedMore(num::NonZero::new(consumed)),
        };
        let mut next = *self;
        next.framer = framer;
        Ok(Plan {
            event: Prepared::Event { next, event },
        })
    }

    pub fn decode<'a>(&self, buf: &'a [u8]) -> Result<Plan<'a>, Error> {
        self.decode_max(buf, usize::MAX)
    }

    pub(crate) fn require<'field>(buf: &[u8], max_body: usize) -> Result<Output<'field>, Error> {
        Output::drain(buf, max_body)?.ok_or(Error::Incomplete)
    }

    pub fn body(buf: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(Output::drain(buf, MAX_BODY_SIZE)?.map(|result| result.body))
    }

    fn parse<'field>(fields: trailer::Fields<'_>) -> Result<List<'field>, Conversion> {
        let mut trailers = List::new();
        for (field, (name, value)) in fields.enumerate() {
            trailers
                .push(name, value)
                .map_err(|cause| Conversion { field, cause })?;
        }
        Ok(trailers)
    }
}

const _: () = {
    assert!(core::mem::size_of::<Error>() <= 5 * core::mem::size_of::<usize>());
    assert!(!core::mem::needs_drop::<Error>());
};
