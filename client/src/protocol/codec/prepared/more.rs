use dope::core::io::transfer;
use o3::buffer::{resident, write::consume::scope};
use sark_http::h1::codec::decode::trailer;

use crate::protocol::{
    Error,
    codec::{
        chunks::{Chunks, Decoding},
        pending,
        state::Framing,
    },
};

type End<'id> = scope::End<'id, { transfer::MAX_BYTES as u32 }>;

pub(in crate::protocol::codec) enum More<'id, 'input, 'd> {
    Ready(Chunks<'d>, Option<End<'id>>),
    Store {
        chunks: Chunks<'d>,
        trailer: trailer::Pending<'input>,
        consumed: End<'id>,
    },
    Append {
        plan: trailer::Plan<'input, 'd, trailer::More<End<'id>>>,
        pending: pending::Pending<'d>,
    },
}

impl<'id, 'input, 'd> More<'id, 'input, 'd> {
    pub(super) fn cancel(self) -> Framing<'d> {
        Framing::Chunked(match self {
            Self::Ready(chunks, _) => chunks,
            Self::Store { chunks, .. } => chunks,
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
    ) -> Result<(Framing<'d>, Option<End<'id>>), Error> {
        match self {
            Self::Ready(chunks, consumed) => Ok((Framing::Chunked(chunks), consumed)),
            Self::Store {
                mut chunks,
                trailer,
                consumed,
            } => {
                let owner = match Chunks::allocate(budget, trailer.len(), trailer.capacity()) {
                    Ok(owner) => owner,
                    Err(error) => {
                        *framing = Framing::Chunked(chunks);
                        return Err(error);
                    }
                };
                let trailers = match trailer.try_store(owner) {
                    Ok(trailers) => trailers,
                    Err(rejected) => {
                        let error = rejected.discard();
                        *framing = Framing::Chunked(chunks);
                        return Err(Error::TrailerStore(error));
                    }
                };
                chunks.decoder = Decoding::Trailers(trailers);
                Ok((Framing::Chunked(chunks), Some(consumed)))
            }
            Self::Append { mut plan, pending } => loop {
                match plan.try_append() {
                    Ok(consumed) => {
                        return Ok((
                            Framing::Chunked(Chunks {
                                decoder: Decoding::Trailers(plan.cancel()),
                                pending,
                            }),
                            consumed,
                        ));
                    }
                    Err(error) => {
                        if let Err(error) = Chunks::grow(budget, &mut plan, error) {
                            *framing = Self::Append { plan, pending }.cancel();
                            return Err(error);
                        }
                    }
                }
            },
        }
    }
}
