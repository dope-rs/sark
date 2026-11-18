pub(in crate::hpack::decode) mod complete;
mod kind;
pub(in crate::hpack::decode) mod literal;
pub(in crate::hpack::decode) mod lookup;
pub(in crate::hpack::decode) mod payload;
pub(in crate::hpack::decode) mod repr;

use dynamic::{insert::stages, mode::Staged, plan, table};
use o3::alloc;
use sark_http::{
    field::metadata::{block::buffer, map::record},
    payload::compress::dynamic,
};

use crate::hpack::{
    capacity,
    decode::{self, limit, mode},
};

pub struct Decoder {
    pub(in crate::hpack::decode) dyn_table: stages::Ready,
    pub(in crate::hpack::decode) core: Core,
}

pub(crate) struct Core {
    pub(in crate::hpack::decode) max_size_setting: usize,
    required_resize: Option<usize>,
    pub(in crate::hpack::decode) max_header_list_size: Option<usize>,
    pub(in crate::hpack::decode) name_scratch: buffer::Names,
    pub(in crate::hpack::decode) value_scratch: buffer::Values,
}

impl Core {
    pub(in crate::hpack::decode) fn field(
        &self,
        budget: &mut limit::Budget,
    ) -> Result<(), decode::Error> {
        budget.close_resize(self.required_resize)
    }

    pub(in crate::hpack::decode) fn check_resize(&self) -> Result<(), decode::Error> {
        match self.required_resize {
            Some(limit) => Err(decode::Error::MissingResize { limit }),
            None => Ok(()),
        }
    }

    pub(in crate::hpack::decode) fn resize(
        &mut self,
        table: &mut stages::Ready,
        budget: &limit::Budget,
        size: usize,
    ) -> Result<(), decode::Error> {
        let limit = self.required_resize.unwrap_or(self.max_size_setting);
        budget.admit_resize(size, limit)?;
        table.edit().set_limit(size);
        self.required_resize = None;
        Ok(())
    }
}

impl Decoder {
    pub(crate) fn budget(&self) -> limit::Budget {
        limit::Budget::new(self.core.max_header_list_size)
    }

    pub fn try_new(size: usize) -> Result<Self, capacity::Error> {
        let plan = plan::Plan::<Staged>::new(size)?;
        let table = plan
            .try_table(plan.capacity())
            .map_err(capacity::Error::from)?;
        Ok(Self::from_table(table, plan.capacity()))
    }

    pub(crate) fn try_from(plan: plan::Plan<Staged>) -> Result<Self, alloc::Error> {
        let initial = crate::hpack::INITIAL_TABLE_SIZE as usize;
        let table = plan.try_table(initial)?;
        Ok(Self::from_table(table, initial))
    }

    fn from_table(table: table::Table<Staged>, max: usize) -> Self {
        Self {
            dyn_table: stages::Ready::new(table),
            core: Core {
                max_size_setting: max,
                required_resize: None,
                max_header_list_size: None,
                name_scratch: buffer::Names::new(),
                value_scratch: buffer::Values::new(),
            },
        }
    }

    /// Applies an acknowledged maximum. The peer's next leading size update
    /// performs any required eviction before another field can be decoded.
    pub fn set_max_size(&mut self, n: usize) {
        let limit = n.min(self.dyn_table.capacity());
        self.core.max_size_setting = limit;
        if limit < self.dyn_table.max_size() {
            self.core.required_resize = Some(
                self.core
                    .required_resize
                    .map_or(limit, |pending| pending.min(limit)),
            );
        }
    }

    pub(crate) fn finish(self) -> Result<Self, decode::Error> {
        self.core.check_resize()?;
        Ok(self)
    }

    pub fn set_max_list(&mut self, n: Option<usize>) {
        self.core.max_header_list_size = n;
    }

    pub fn dyn_size(&self) -> usize {
        self.dyn_table.current_size()
    }

    pub fn dyn_max(&self) -> usize {
        self.dyn_table.max_size()
    }

    pub fn dyn_len(&self) -> usize {
        self.dyn_table.entry_count()
    }

    pub fn dyn_get(&self, index: usize) -> Option<(record::Span<'_>, record::Span<'_>)> {
        self.dyn_table
            .field(index)
            .map(|field| (field.name().span(), field.value()))
    }

    pub fn decode_limit<F>(
        self,
        buf: &[u8],
        mut emit: F,
    ) -> Result<(Self, limit::Admission), decode::Error>
    where
        F: FnMut(record::Span<'_>, record::Span<'_>),
    {
        let mut budget = self.budget();
        let (decoder, _) = self
            .decode_to::<mode::Emit, _>(buf, &mut budget, &mut emit)
            .map_err(|(_, cause)| cause)?;
        Ok((decoder, budget.admission))
    }

    pub(crate) fn discard(self, buf: &[u8]) -> Result<Self, decode::Error> {
        let (decoder, ()) = self
            .decode_to::<mode::Discard, _>(buf, &mut limit::Budget::new(None), ())
            .map_err(|((), cause)| cause)?;
        Ok(decoder)
    }

    pub(crate) fn decode_to<M, S>(
        mut self,
        mut input: &[u8],
        budget: &mut limit::Budget,
        sink: S,
    ) -> Result<(Self, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        match complete::run::<M, S>(&mut self.core, self.dyn_table, &mut input, budget, sink) {
            complete::Outcome::Complete(table, sink) => {
                let ((), sink) = mode::carry(sink, self.core.check_resize())?;
                self.dyn_table = table;
                Ok((self, sink))
            }
            complete::Outcome::NeedMore(_, _, sink) => Err((sink, decode::Error::NeedMore.into())),
            complete::Outcome::Failed(sink, cause) => Err((sink, cause)),
        }
    }
}
