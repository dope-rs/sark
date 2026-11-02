pub(in crate::hpack::decode) mod complete;
mod kind;
pub(in crate::hpack::decode) mod literal;
pub(in crate::hpack::decode) mod lookup;
pub(in crate::hpack::decode) mod payload;
pub(in crate::hpack::decode) mod repr;

use core::ops;
use std::num::NonZero;

use dynamic::{
    insert::{self, stages},
    mode::Staged,
    plan, table,
};
use o3::alloc;
use sark_http::{
    field::metadata::{block::buffer, map::record},
    payload::compress::dynamic,
};

use crate::hpack::{
    capacity,
    decode::{self, limit, mode},
    statics,
};

pub struct Decoder(pub(in crate::hpack::decode) Machine<stages::Ready>);

pub(crate) struct Machine<T> {
    pub(in crate::hpack::decode) dyn_table: T,
    pub(in crate::hpack::decode) core: Core,
}

pub(crate) struct Core {
    pub(in crate::hpack::decode) max_size_setting: usize,
    required_resize: Option<usize>,
    pub(in crate::hpack::decode) max_header_list_size: Option<usize>,
    pub(in crate::hpack::decode) name_scratch: buffer::Names,
    pub(in crate::hpack::decode) value_scratch: buffer::Values,
}

pub(in crate::hpack::decode) enum Insert {
    Name(Machine<stages::Name>),
    Ready(Machine<stages::Ready>),
}

impl<T> Machine<T> {
    pub(in crate::hpack::decode) fn parts(self) -> (T, Core) {
        (self.dyn_table, self.core)
    }
}

impl Machine<stages::Ready> {
    pub(in crate::hpack::decode) fn begin(self) -> Insert {
        let (table, core) = self.parts();
        match table.begin() {
            stages::Begin::Name(table) => Insert::Name(core.with(table)),
            stages::Begin::Omitted(table) => Insert::Ready(core.with(table)),
        }
    }
}

impl Machine<stages::Name> {
    pub(in crate::hpack::decode) fn copy_name(
        self,
        index: NonZero<usize>,
    ) -> Result<literal::Path, decode::Error> {
        let (table, core) = self.parts();
        let index = index.get();
        let fixed = statics::TABLE.len();
        let copied = if index <= fixed {
            let Some(field) = statics::TABLE.get(index) else {
                return Err(decode::Error::BadIndex);
            };
            table.set_name(field.name())
        } else {
            table.copy(index - fixed - 1)
        };
        match copied {
            Ok(table) => Ok(literal::Path::Staged(core.with(table))),
            Err((table, insert::Error::Capacity { .. } | insert::Error::Length { .. })) => {
                Ok(literal::Path::Ready(Decoder(core.with(table.discard()))))
            }
            Err((_, error)) => Err(match error {
                insert::Error::Index { .. } => decode::Error::BadIndex,
                error => decode::Error::from(error),
            }),
        }
    }

    pub(in crate::hpack::decode) fn finish(self) -> Result<Machine<stages::Value>, decode::Error> {
        let (table, core) = self.parts();
        match table.finish() {
            Ok(table) => Ok(core.with(table)),
            Err((_, error)) => Err(error.into()),
        }
    }

    pub(in crate::hpack::decode) fn discard(self) -> Machine<stages::Ready> {
        let (table, core) = self.parts();
        core.with(table.discard())
    }
}

impl Machine<stages::Value> {
    pub(in crate::hpack::decode) fn commit(self) -> Machine<stages::Ready> {
        let (table, core) = self.parts();
        core.with(table.commit())
    }

    pub(in crate::hpack::decode) fn discard(self) -> Machine<stages::Ready> {
        let (table, core) = self.parts();
        core.with(table.discard())
    }
}

impl Core {
    pub(in crate::hpack::decode) fn field(
        &self,
        budget: &mut limit::Budget,
    ) -> Result<(), decode::Error> {
        budget.close_resize(self.required_resize)
    }

    fn check_resize(&self) -> Result<(), decode::Error> {
        match self.required_resize {
            Some(limit) => Err(decode::Error::MissingResize { limit }),
            None => Ok(()),
        }
    }

    pub(in crate::hpack::decode) fn with<T>(self, table: T) -> Machine<T> {
        Machine {
            dyn_table: table,
            core: self,
        }
    }
}

impl<T> ops::Deref for Machine<T> {
    type Target = Core;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl<T> ops::DerefMut for Machine<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
}

impl Decoder {
    pub(crate) fn budget(&self) -> limit::Budget {
        limit::Budget::new(self.0.core.max_header_list_size)
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
        Self(Machine {
            dyn_table: stages::Ready::new(table),
            core: Core {
                max_size_setting: max,
                required_resize: None,
                max_header_list_size: None,
                name_scratch: buffer::Names::new(),
                value_scratch: buffer::Values::new(),
            },
        })
    }

    /// Applies an acknowledged maximum. The peer's next leading size update
    /// performs any required eviction before another field can be decoded.
    pub fn set_max_size(&mut self, n: usize) {
        let limit = n.min(self.0.dyn_table.capacity());
        self.0.core.max_size_setting = limit;
        if limit < self.0.dyn_table.max_size() {
            self.0.core.required_resize = Some(
                self.0
                    .core
                    .required_resize
                    .map_or(limit, |pending| pending.min(limit)),
            );
        }
    }

    pub(in crate::hpack::decode) fn resize(
        &mut self,
        budget: &limit::Budget,
        size: usize,
    ) -> Result<(), decode::Error> {
        let limit = self
            .0
            .core
            .required_resize
            .unwrap_or(self.0.core.max_size_setting);
        budget.admit_resize(size, limit)?;
        self.0.dyn_table.edit().set_limit(size);
        self.0.core.required_resize = None;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Self, decode::Error> {
        self.0.core.check_resize()?;
        Ok(self)
    }

    pub fn set_max_list(&mut self, n: Option<usize>) {
        self.0.core.max_header_list_size = n;
    }

    pub fn dyn_size(&self) -> usize {
        self.0.dyn_table.current_size()
    }

    pub fn dyn_max(&self) -> usize {
        self.0.dyn_table.max_size()
    }

    pub fn dyn_len(&self) -> usize {
        self.0.dyn_table.entry_count()
    }

    pub fn dyn_get(&self, index: usize) -> Option<(record::Span<'_>, record::Span<'_>)> {
        self.0
            .dyn_table
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
        let (decoder, _) = complete::Complete::new(self)
            .block::<mode::Emit, _>(buf, &mut budget, &mut emit)
            .map_err(|(_, cause)| cause)?;
        Ok((decoder, budget.admission))
    }

    pub(crate) fn discard(self, buf: &[u8]) -> Result<Self, decode::Error> {
        complete::Complete::new(self)
            .block::<mode::Discard, _>(buf, &mut limit::Budget::new(None), ())
            .map(|(decoder, ())| decoder)
            .map_err(|((), cause)| cause)
    }

    pub(crate) fn decode_to<M, S>(
        self,
        buf: &[u8],
        sink: S,
    ) -> Result<(Self, limit::Admission, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let mut budget = self.budget();
        let (decoder, sink) =
            complete::Complete::new(self).block::<M, S>(buf, &mut budget, sink)?;
        Ok((decoder, budget.admission, sink))
    }
}
