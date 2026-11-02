use std::{mem, task::Poll};

use o3::buffer::storage;

use crate::Error;

pub(super) mod admitted;
pub(super) mod project;
pub(super) mod release;
pub(super) mod value;

use crate::segment::{Project, Release, input::Input, state};
use value::Value;

/// Parsed field index that continues to own every original transport segment.
pub struct Document<'d> {
    pub(super) input: Input<'d>,
    pub(super) fields: Vec<Option<state::Value>>,
}

impl<'d> Document<'d> {
    pub(super) fn new(input: Input<'d>, fields: Vec<Option<state::Value>>) -> Self {
        Self { input, fields }
    }

    /// Retires field slots and input owners within the remaining work budget.
    pub fn retire(&mut self, budget: &mut usize) -> Poll<()> {
        self.drain(budget).0
    }

    #[inline(always)]
    pub(super) fn drain(&mut self, budget: &mut usize) -> (Poll<()>, super::scanner::Released) {
        while *budget != 0 && self.fields.pop().is_some() {
            *budget -= 1;
        }
        if self.fields.is_empty() {
            self.input.release(budget)
        } else {
            (Poll::Pending, super::scanner::Released::new())
        }
    }

    /// Ends projection and retains the remaining owners for bounded release.
    pub fn release(self) -> Release<'d> {
        Release { document: self }
    }

    /// Borrows one field and its original input after a single table lookup.
    /// A missing field is distinct from an invalid index or a JSON null value.
    pub fn field(&self, field: usize) -> crate::Result<Option<Value<'_, 'd>>> {
        Ok(self
            .fields
            .get(field)
            .ok_or(Error::Invalid)?
            .as_ref()
            .map(|value| Value::new(&self.input, value)))
    }

    pub fn u64(&self, field: usize) -> crate::Result<u64> {
        self.value(field)?.u64()
    }

    pub fn raw(&self, field: usize) -> crate::Result<crate::Raw<storage::Shared>> {
        self.value(field)?.raw()
    }

    /// Starts a cooperative transfer of one already-validated compound.
    /// Selection failures retain the document until budgeted retirement finishes.
    pub fn project(self, field: usize) -> Project<'d> {
        Project::new(self, field)
    }

    /// Selects and projects one compound without repeating its field lookup.
    /// A missing field returns None; a present non-compound remains invalid.
    /// Rejection preserves this document; only a successful transfer empties it.
    pub fn project_at(&mut self, field: usize) -> crate::Result<Option<Project<'d>>> {
        let Some(value) = self.field(field)? else {
            return Ok(None);
        };
        let parts = value.split()?;
        Ok(Some(Project::with_parts(
            mem::replace(self, Self::new(Input::empty(), Vec::new())),
            parts,
        )))
    }

    fn value(&self, field: usize) -> crate::Result<Value<'_, 'd>> {
        self.field(field)?.ok_or(Error::Invalid)
    }
}
