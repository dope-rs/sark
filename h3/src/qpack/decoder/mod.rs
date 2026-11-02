use core::{convert::Infallible, marker};

use dynamic::{mode, plan};
use ingress::error;
use o3::{
    alloc,
    buffer::{pool, write::consume},
};
use sark_http::{
    field::metadata::{
        block,
        block::buffer,
        integer, lower,
        lower::plain,
        packed,
        section::{
            Policy, rules,
            state::{RetainedHead, Selected, complete, decode},
            storage,
        },
    },
    payload::compress::dynamic,
};

use crate::{
    qpack,
    qpack::{dynamics, identity::Identity, statics},
    sources,
};

mod fields;
pub mod ingress;
pub(super) mod inputs;
mod lines;
pub(crate) mod state;

pub(crate) enum Prepared<'a, S: crate::Source + ?Sized> {
    ReadyContiguous(inputs::DecodePlan<&'a [u8]>),
    ReadySegmented(inputs::DecodePlan<inputs::Segmented<'a, S>>),
    Blocked(qpack::Reference),
}

pub(crate) enum Message<S, B> {
    Ready {
        head: RetainedHead<B, S, Option<block::Plan>>,
        validation: block::Validation,
        response: Option<block::Response>,
    },
    Blocked(qpack::Reference),
}

type Decoded<P> = Message<Selected<P>, <P as Policy>::Block>;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Error {
    Allocation(alloc::Error),
    FieldWrite(consume::Error),
    FieldValidation {
        planned: block::Validation,
        sealed: block::Validation,
    },
    Lower(lower::Error),
    Incomplete {
        available: usize,
    },
    Integer {
        prefix: u8,
        available: usize,
        cause: integer::Overflow,
    },
    Required {
        encoded: u64,
        inserted: u64,
        maximum: u64,
    },
    Base {
        required: u64,
        delta: u64,
        sign: qpack::DeltaSign,
    },
    Blocked {
        required: qpack::Reference,
    },
    BlockedConflict {
        stream_id: u64,
        current: qpack::Reference,
        required: qpack::Reference,
    },
    BlockedLimit {
        stream_id: u64,
        required: qpack::Reference,
        limit: usize,
    },
    StaticReference {
        index: u64,
    },
    DynamicDisabled {
        byte: u8,
        index: u64,
    },
    DynamicReference(dynamics::Reference),
    FieldCapacity {
        available: usize,
        required: usize,
    },
    LiteralLength {
        encoded: u64,
        available: usize,
    },
    LiteralCapacity {
        written: usize,
        additional: usize,
        maximum: usize,
    },
    LiteralEncoding,
    LiteralDecodeInput {
        expected: usize,
        actual: usize,
    },
    PoolExhausted(alloc::Exhausted),
    PoolEmpty,
}

impl From<alloc::Error> for Error {
    fn from(error: alloc::Error) -> Self {
        Self::Allocation(error)
    }
}

impl From<alloc::Exhausted> for Error {
    fn from(error: alloc::Exhausted) -> Self {
        Self::PoolExhausted(error)
    }
}

impl From<consume::Error> for Error {
    fn from(error: consume::Error) -> Self {
        Self::FieldWrite(error)
    }
}

impl From<plain::Error> for Error {
    fn from(error: plain::Error) -> Self {
        Self::Lower(error.into())
    }
}

impl From<lower::Error> for Error {
    fn from(error: lower::Error) -> Self {
        Self::Lower(error)
    }
}

impl From<statics::Error> for Error {
    fn from(error: statics::Error) -> Self {
        match error {
            statics::Error::Reference { index } => Self::StaticReference { index },
        }
    }
}

impl From<Infallible> for Error {
    fn from(error: Infallible) -> Self {
        match error {}
    }
}

impl Error {
    pub(super) fn integer<const BITS: u8>(len: usize, error: integer::Error) -> Self {
        match error {
            integer::Error::NeedMore => Self::Incomplete { available: len },
            integer::Error::Overflow(cause) => Self::Integer {
                prefix: BITS,
                available: len,
                cause,
            },
        }
    }
}

pub struct Decoder {
    max_field_section_size: usize,
    encoder_stream: ingress::State,
    field_pool: pool::Growing,
    name_scratch: buffer::Names,
    value_scratch: buffer::Values,
    head_scratch: storage::Scratch,
}

pub(crate) const FEEDBACK_CAPACITY: usize = state::CAPACITY;

impl Decoder {
    pub(crate) const DEFAULT_FIELD_BLOCKS: usize = 1024;

    pub fn try_new(limit: usize) -> Result<Self, qpack::Create> {
        Self::try_dynamic(limit, 0)
    }

    pub fn try_dynamic(limit: usize, max: usize) -> Result<Self, qpack::Create> {
        use qpack::{Create, Storage};

        let layout =
            pool::Layout::new(Self::DEFAULT_FIELD_BLOCKS, limit.max(1)).map_err(Create::Fields)?;
        let blocks = pool::Growing::try_new(layout)
            .map_err(|error| Create::Storage(Storage::Pool(error)))?;
        let plan = plan::Plan::<mode::Staged>::new(max).map_err(Create::Table)?;
        let table = plan
            .try_table(0)
            .map_err(|error| Create::Storage(Storage::Table(error)))?;
        Ok(Self::from_parts(
            limit,
            dynamics::Table::from_table(table, max),
            blocks,
        ))
    }

    pub(crate) fn try_plans(
        limit: usize,
        table: plan::Plan<mode::Staged>,
        blocks: pool::Layout,
    ) -> Result<Self, qpack::Storage> {
        use qpack::Storage;

        let entries = table.try_table(0).map_err(Storage::Table)?;
        Ok(Self::from_parts(
            limit,
            dynamics::Table::from_table(entries, table.capacity()),
            pool::Growing::try_new(blocks).map_err(Storage::Pool)?,
        ))
    }

    fn from_parts(
        limit: usize,
        table: dynamics::Table<mode::Staged>,
        blocks: pool::Growing,
    ) -> Self {
        Self {
            max_field_section_size: limit,
            encoder_stream: ingress::State::new(table),
            field_pool: blocks,
            name_scratch: buffer::Names::new(),
            value_scratch: buffer::Values::new(),
            head_scratch: storage::Scratch::default(),
        }
    }

    pub fn insert_count(&self) -> u64 {
        self.encoder_stream.table().insert_count()
    }

    pub fn ingest(&mut self, buf: &[u8]) -> Result<usize, error::Error> {
        let mut feedback = state::State::new();
        let (progress, _) = self
            .encoder_stream
            .ingest(&mut feedback, FEEDBACK_CAPACITY, buf);
        let progress = progress?;
        if let state::Status::Blocked(capacity) = progress.feedback {
            return Err(error::Error::FeedbackBlocked(capacity));
        }
        Ok(progress.consumed)
    }

    pub(crate) fn ingest_limit(
        &mut self,
        feedback: &mut state::State,
        buf: &[u8],
        limit: usize,
    ) -> (Result<qpack::Progress, error::Error>, qpack::Growth) {
        self.encoder_stream.ingest(feedback, limit, buf)
    }

    pub fn decode(&mut self, buf: &[u8]) -> Result<packed::Decoded, Error> {
        let prepared = self.prepare(buf, &mut None)?;
        match prepared {
            Prepared::ReadyContiguous(plan) => self.build_raw(plan),
            Prepared::ReadySegmented(plan) => self.build_raw(plan),
            Prepared::Blocked(required) => Err(Error::Blocked { required }),
        }
    }

    pub(crate) fn decode_head<P: rules::Policy, A: rules::Push, S: crate::Source + ?Sized>(
        &mut self,
        prepared: Prepared<'_, S>,
        kind: rules::Kind,
        connect: block::Connect,
        identity: Option<&mut Identity>,
    ) -> Result<Decoded<P>, Error> {
        let finished = match prepared {
            Prepared::ReadyContiguous(plan) => {
                self.decode_plan::<P, A, _>(plan, kind, connect, identity)?
            }
            Prepared::ReadySegmented(plan) => {
                self.decode_plan::<P, A, _>(plan, kind, connect, identity)?
            }
            Prepared::Blocked(required) => {
                return Ok(Message::Blocked(required));
            }
        };
        let (head, sealed, finished, response) = finished.into_parts();
        if finished != sealed {
            return Err(Error::FieldValidation {
                planned: finished,
                sealed,
            });
        }
        Ok(Message::Ready {
            head,
            validation: sealed,
            response,
        })
    }

    pub(crate) fn prepare<'a, S: crate::Source + ?Sized>(
        &self,
        buf: &'a S,
        retained: &mut Option<qpack::Reference>,
    ) -> Result<Prepared<'a, S>, Error> {
        let table = self.encoder_stream.table();
        let inserts = table.insert_count();
        let max = table.max_entries();
        match buf.form() {
            sources::Form::Contiguous(bytes) => {
                match inputs::Prepared::parse(bytes, inserts, max, retained)? {
                    inputs::Prepared::Ready(plan) => Ok(Prepared::ReadyContiguous(plan)),
                    inputs::Prepared::Blocked(required) => Ok(Prepared::Blocked(required)),
                }
            }
            sources::Form::Segmented => {
                match inputs::Prepared::parse(inputs::Segmented::new(buf), inserts, max, retained)?
                {
                    inputs::Prepared::Ready(plan) => Ok(Prepared::ReadySegmented(plan)),
                    inputs::Prepared::Blocked(required) => Ok(Prepared::Blocked(required)),
                }
            }
        }
    }

    fn build_raw<'a, I>(&mut self, plan: inputs::DecodePlan<I>) -> Result<packed::Decoded, Error>
    where
        I: inputs::Input<'a>,
    {
        let (input, required, base) = plan.into_parts();
        let fields = self.field_pool.try_acquire()?.ok_or(Error::PoolEmpty)?;
        let mut fields = storage::Writer::try_new(fields)?;
        let mut output = fields::RawOutput::new(&mut fields, self.max_field_section_size);
        let lines = lines::Lines {
            input,
            base,
            output: &mut output,
        };
        match required {
            qpack::Requirement::Static => lines.decode(lines::Static)?,
            qpack::Requirement::Dynamic(required) => {
                lines.decode(lines::Dynamic::new(&self.encoder_stream.table(), required))?
            }
        }
        Ok(<packed::Decoded as storage::Block>::from(fields.finish()))
    }

    fn decode_plan<'a, P, A, I>(
        &mut self,
        plan: inputs::DecodePlan<I>,
        kind: rules::Kind,
        connect: block::Connect,
        identity: Option<&mut Identity>,
    ) -> Result<decode::Final<P>, Error>
    where
        P: rules::Policy,
        A: rules::Push,
        I: inputs::Input<'a>,
    {
        let (input, required, base) = plan.into_parts();

        let fields = self.field_pool.try_acquire()?.ok_or(Error::PoolEmpty)?;
        let consumer =
            complete::Decoder::<P>::try_new(fields, &mut self.head_scratch, kind, connect)?;
        let mut scratch = fields::Scratch::new(&mut self.name_scratch, &mut self.value_scratch);
        let mut output =
            fields::FieldOutput::<P>::new(consumer, self.max_field_section_size, identity);
        let lines = lines::Lines {
            input,
            base,
            output: lines::Fields::<P, A> {
                output: &mut output,
                scratch: &mut scratch,
                push: marker::PhantomData,
            },
        };
        match required {
            qpack::Requirement::Static => lines.decode(lines::Static)?,
            qpack::Requirement::Dynamic(required) => {
                lines.decode(lines::Dynamic::new(&self.encoder_stream.table(), required))?
            }
        }
        output.finish()
    }
}

const _: () = assert!(size_of::<Error>() == 9 * size_of::<usize>());
const _: () = {
    assert!(<packed::Decoded as storage::Block>::PREFIX.is_empty());
    assert!(<storage::Fields as storage::Block>::PREFIX.is_empty());
};
