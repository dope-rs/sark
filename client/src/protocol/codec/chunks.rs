use dope::{core::io::transfer, manifold::client::session::codec};
use o3::{
    buffer,
    buffer::{
        hold,
        hold::scope::Bound,
        resident,
        storage::Owned,
        write::consume::{self, scope},
    },
};
use sark_http::h1::codec::decode::{chunks, trailer};

use crate::protocol::{
    Error,
    codec::{
        output::{self, Completion, Output},
        pending,
        prepared::{Item, More, Stage},
        retain,
        state::Framing,
    },
    response,
};

/// Transfer-decoding state for one chunked response body.
pub(super) struct Chunks<'d> {
    pub(super) decoder: Decoding<'d>,
    pub(super) pending: pending::Pending<'d>,
}

pub(super) enum Decoding<'d> {
    Body(chunks::Decoder),
    Trailers(trailer::State<'d>),
}

type End<'id> = scope::End<'id, { transfer::MAX_BYTES as u32 }>;

enum BodyStep<'id, 'input, 'd, R: buffer::Cursor + 'input> {
    More(Option<End<'id>>),
    Trailers(trailer::Pending<'input>, End<'id>),
    Chunk(&'input [u8], End<'id>),
    Done(Option<Bound<'input, 'd, R>>, End<'id>),
}

impl<'d> Chunks<'d> {
    pub(super) fn new(max_body: usize) -> Self {
        Self {
            decoder: Decoding::Body(chunks::Decoder::with_limit(max_body)),
            pending: pending::Pending::new(),
        }
    }

    pub(super) fn prepare<'id, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        self,
        framing: &mut Framing<'d>,
        buf: &codec::Input<'id, 'input, 'd, R>,
    ) -> Result<Stage<'id, 'input, 'd, R>, Error> {
        let Self { decoder, pending } = self;
        let stage = match decoder {
            Decoding::Body(decoder) => Self::body(decoder, pending, framing, buf),
            Decoding::Trailers(state) => Self::trailers(state, pending, framing, buf),
        }?;
        *framing = Framing::Failed;
        Ok(stage)
    }

    fn body<'id, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        mut decoder: chunks::Decoder,
        pending: pending::Pending<'d>,
        framing: &mut Framing<'d>,
        buf: &codec::Input<'id, 'input, 'd, R>,
    ) -> Result<Stage<'id, 'input, 'd, R>, Error> {
        let mark = decoder;
        let step = (|| {
            let plan = decoder
                .decode_max(buf.as_slice(), output::DATA_LIMIT)
                .map_err(Error::Chunk)?;
            let event = match plan.into_trailer() {
                Ok(trailer) => {
                    let consumed = buf.try_consumed(trailer.consumed()).map_err(Error::Input)?;
                    return Ok(BodyStep::Trailers(trailer, consumed));
                }
                Err(plan) => plan.commit(&mut decoder),
            };
            match event {
                chunks::Event::NeedMore(consumed) => {
                    let consumed = consumed
                        .map(|count| buf.try_consumed(count))
                        .transpose()
                        .map_err(Error::Input)?;
                    Ok(BodyStep::More(consumed))
                }
                chunks::Event::Chunk {
                    consumed, bytes, ..
                } => {
                    let consumed = buf.try_consumed(consumed).map_err(Error::Input)?;
                    Ok(BodyStep::Chunk(bytes, consumed))
                }
                chunks::Event::Done { consumed, fields } => {
                    let consumed = buf.try_consumed(consumed).map_err(Error::Input)?;
                    let section = fields.section();
                    let fields = if section.is_empty() {
                        None
                    } else {
                        let start = section.as_ptr().addr() - buf.as_ptr().addr();
                        Some(
                            buf.try_plan(start..start + section.len())
                                .map_err(Error::Range)?
                                .bind(retain::RESPONSE),
                        )
                    };
                    Ok(BodyStep::Done(fields, consumed))
                }
            }
        })();
        let state = Self {
            decoder: Decoding::Body(mark),
            pending,
        };
        let step = match step {
            Ok(step) => step,
            Err(error) => {
                *framing = Framing::Chunked(state);
                return Err(error);
            }
        };
        Ok(match step {
            BodyStep::More(consumed) => Stage::More(More::Ready(
                Self {
                    decoder: Decoding::Body(decoder),
                    ..state
                },
                consumed,
            )),
            BodyStep::Trailers(trailer, consumed) => Stage::More(More::Store {
                chunks: state,
                trailer,
                consumed,
            }),
            BodyStep::Chunk(bytes, consumed) => {
                Stage::Item(Item::chunk(state, decoder, bytes, consumed, framing, buf)?)
            }
            BodyStep::Done(fields, consumed) => Stage::Item(Item::Complete {
                fields,
                chunks: state,
                consumed,
            }),
        })
    }

    fn trailers<'id, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        state: trailer::State<'d>,
        pending: pending::Pending<'d>,
        framing: &mut Framing<'d>,
        buf: &codec::Input<'id, 'input, 'd, R>,
    ) -> Result<Stage<'id, 'input, 'd, R>, Error> {
        let plan = match state.prepare(buf.as_slice()) {
            Ok(plan) => plan,
            Err(rejected) => {
                let (state, error) = rejected.into_parts();
                *framing = Framing::Chunked(Self {
                    decoder: Decoding::Trailers(state),
                    pending,
                });
                return Err(Error::Trailer(error));
            }
        };
        let plan = match plan.try_map(|count| buf.try_consumed(count)) {
            Ok(plan) => plan,
            Err(rejected) => {
                let (state, error) = rejected.into_parts();
                *framing = Framing::Chunked(Self {
                    decoder: Decoding::Trailers(state),
                    pending,
                });
                return Err(Error::Input(error));
            }
        };
        Ok(match plan.split() {
            Ok(plan) => Stage::Item(Item::Append { plan, pending }),
            Err(plan) => Stage::More(More::Append { plan, pending }),
        })
    }

    pub(super) fn grow<P>(
        budget: resident::Budget<'d>,
        plan: &mut trailer::Plan<'_, 'd, P>,
        error: consume::Error,
    ) -> Result<(), Error> {
        let (required, _) = error.into_parts();
        let owner = Self::allocate(budget, plan.len() + required.get(), usize::MAX)?;
        plan.try_grow(owner)
            .map_err(|rejected| Error::TrailerStore(rejected.discard()))
    }

    /// Bounds retained capacity after the final empty line is removed.
    pub(super) fn allocate(
        budget: resident::Budget<'d>,
        required: usize,
        maximum: usize,
    ) -> Result<Owned<resident::Budget<'d>>, Error> {
        let resident = required
            .saturating_sub(2)
            .saturating_mul(retain::MAX_RESIDENT_AMPLIFICATION)
            .max(required);
        let capacity = required
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX)
            .min(maximum)
            .min(resident)
            .min(budget.available().max(required));
        Owned::try_allocate(&budget, capacity)
            .map_err(|error| Error::Retention(hold::Error::Resident(error)))
    }

    pub(super) fn complete(
        data: Option<resident::Shared<'d>>,
        fields: Option<response::Fields<resident::Shared<'d>>>,
    ) -> Output<'d> {
        let data = data.map(|data| Ok(response::Event::Data(response::Data::from_owner(data))));
        let trailer = fields.map(|fields| Ok(response::Event::Trailers(fields)));
        Output::events([data, trailer], Completion::Complete)
    }
}
