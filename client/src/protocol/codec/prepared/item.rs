use dope::{core::io::transfer, manifold::client::session::codec};
use o3::buffer::{self, hold, hold::scope::Bound, resident, write::consume::scope};
use sark_http::h1::codec::decode::{chunks, trailer};

use crate::protocol::{
    Error,
    codec::{
        chunks::{Chunks, Decoding},
        output::{self, Completion, Output, Parsed, Preview},
        pending, retain,
        state::Framing,
    },
    response,
};

type End<'id> = scope::End<'id, { transfer::MAX_BYTES as u32 }>;

pub(in crate::protocol::codec) struct Chunk<'id, 'input, 'd, R: buffer::Cursor + 'input> {
    chunks: Chunks<'d>,
    next: chunks::Decoder,
    bytes: &'input [u8],
    copied: &'input [u8],
    tail: Option<Bound<'input, 'd, R>>,
    consumed: End<'id>,
}

pub(in crate::protocol::codec) enum Item<'id, 'input, 'd, R: buffer::Cursor + 'input> {
    Chunk(Chunk<'id, 'input, 'd, R>),
    Complete {
        fields: Option<Bound<'input, 'd, R>>,
        chunks: Chunks<'d>,
        consumed: End<'id>,
    },
    Append {
        plan: trailer::Plan<'input, 'd, trailer::Ready<End<'id>>>,
        pending: pending::Pending<'d>,
    },
}

impl<'id, 'input, 'd, R: buffer::Cursor + hold::raw::Retain<'d>> Item<'id, 'input, 'd, R> {
    pub(in crate::protocol::codec) fn chunk(
        chunks: Chunks<'d>,
        next: chunks::Decoder,
        bytes: &'input [u8],
        consumed: End<'id>,
        framing: &mut Framing<'d>,
        input: &codec::Input<'id, 'input, 'd, R>,
    ) -> Result<Self, Error> {
        let count = if chunks.pending.is_empty() && bytes.len() == output::DATA_LIMIT {
            0
        } else {
            bytes.len().min(chunks.pending.remaining())
        };
        let (copied, tail) = bytes.split_at(count);
        let tail = if tail.is_empty() {
            None
        } else {
            let start = tail.as_ptr().addr() - input.as_ptr().addr();
            let bound = match input.try_plan(start..start + tail.len()) {
                Ok(bound) => bound.bind(retain::RESPONSE),
                Err(error) => {
                    *framing = Framing::Chunked(chunks);
                    return Err(Error::Range(error));
                }
            };
            Some(bound)
        };
        Ok(Self::Chunk(Chunk {
            chunks,
            next,
            bytes,
            copied,
            tail,
            consumed,
        }))
    }

    pub(super) fn preview(&self) -> Preview<'_> {
        let (pending, data, trailers, end) = match self {
            Self::Chunk(chunk) => (
                chunk.chunks.pending.as_slice(),
                chunk.bytes,
                None,
                Completion::Pending,
            ),
            Self::Complete { fields, chunks, .. } => (
                chunks.pending.as_slice(),
                &[][..],
                fields.as_ref().map(|fields| [fields.as_slice(), &[]]),
                Completion::Complete,
            ),
            Self::Append { plan, pending, .. } => {
                let fields = plan.fields();
                (
                    pending.as_slice(),
                    &[][..],
                    fields
                        .iter()
                        .any(|field| !field.is_empty())
                        .then_some(fields),
                    Completion::Complete,
                )
            }
        };
        Preview::Body {
            pending,
            data,
            trailers,
            completion: end,
        }
    }

    pub(super) fn cancel(self) -> Framing<'d> {
        Framing::Chunked(match self {
            Self::Chunk(chunk) => chunk.chunks,
            Self::Complete { chunks, .. } => chunks,
            Self::Append { plan, pending } => Chunks {
                decoder: Decoding::Trailers(plan.cancel()),
                pending,
            },
        })
    }

    pub(super) fn try_commit(
        self,
        framing: &mut Framing<'d>,
        budget: resident::Budget<'d>,
    ) -> Result<(Framing<'d>, Parsed<'d>, End<'id>), Error> {
        let (data, fields, consumed) = match self {
            Self::Chunk(chunk) => return chunk.try_commit(framing, budget),
            Self::Complete {
                fields,
                mut chunks,
                consumed,
            } => {
                let fields = match fields.map(Bound::try_into).transpose() {
                    Ok(fields) => fields.map(response::Fields::admit),
                    Err(error) => {
                        *framing = Framing::Chunked(chunks);
                        return Err(Error::Retention(error));
                    }
                };
                (chunks.pending.finish()?, fields, consumed)
            }
            Self::Append {
                mut plan,
                mut pending,
            } => loop {
                let error = match plan.try_append() {
                    Ok(append) => match pending.finish() {
                        Ok(data) => {
                            let (owner, consumed) = append.commit();
                            let owner = owner.into_owner();
                            let fields = (!owner.is_empty())
                                .then(|| response::Fields::admit(owner.freeze()));
                            break (data, fields, consumed);
                        }
                        Err(error) => {
                            plan = append.cancel();
                            error
                        }
                    },
                    Err(rejected) => {
                        let (retained, error) = rejected.into_parts();
                        plan = retained;
                        match Chunks::grow(budget, &mut plan, error) {
                            Ok(()) => continue,
                            Err(error) => error,
                        }
                    }
                };
                *framing = Self::Append { plan, pending }.cancel();
                return Err(error);
            },
        };
        Ok((
            Framing::Head,
            Chunks::complete(data, fields).into(),
            consumed,
        ))
    }
}

impl<'id, 'input, 'd, R: buffer::Cursor + hold::raw::Retain<'d>> Chunk<'id, 'input, 'd, R> {
    fn try_commit(
        mut self,
        framing: &mut Framing<'d>,
        budget: resident::Budget<'d>,
    ) -> Result<(Framing<'d>, Parsed<'d>, End<'id>), Error> {
        let retained = match self.tail.map(Bound::try_into).transpose() {
            Ok(retained) => retained,
            Err(error) => {
                *framing = Framing::Chunked(self.chunks);
                return Err(Error::Retention(error));
            }
        };
        if !self.copied.is_empty()
            && let Err(error) = self.chunks.pending.extend(budget, self.copied)
        {
            *framing = Framing::Chunked(self.chunks);
            return Err(error);
        }
        let first = self.chunks.pending.take_full();
        let events = [first, retained].map(|data| {
            data.map(|data| Ok(response::Event::Data(response::Data::from_owner(data))))
        });
        self.chunks.decoder = Decoding::Body(self.next);
        Ok((
            Framing::Chunked(self.chunks),
            Output::events(events, Completion::Pending).into(),
            self.consumed,
        ))
    }
}
