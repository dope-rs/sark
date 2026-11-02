use std::{num::NonZero, task::Poll};

use crate::segment::{Admitted, Document, Outcome, input::Split};

/// Retains the source document until its admitted child transfer completes.
pub struct Project<'d> {
    pub(in crate::segment) doc: Document<'d>,
    parts: Option<Split<'d>>,
    field: usize,
    error: Option<Result<(), crate::parsing::Error<'d>>>,
}

impl<'d> Project<'d> {
    pub(in crate::segment) fn new(doc: Document<'d>, field: usize) -> Self {
        Self {
            doc,
            parts: None,
            field,
            error: None,
        }
    }

    pub(in crate::segment) fn with_parts(doc: Document<'d>, parts: Split<'d>) -> Self {
        Self {
            doc,
            parts: Some(parts),
            field: 0,
            error: None,
        }
    }

    pub fn release(&mut self, budget: &mut usize) -> Poll<()> {
        self.error.get_or_insert(Ok(()));
        if let Some(parts) = &mut self.parts
            && parts.release(budget).is_pending()
        {
            return Poll::Pending;
        }
        self.doc.retire(budget)
    }

    pub fn step(
        mut self,
        budget: NonZero<usize>,
    ) -> Outcome<'d, Self, (Document<'d>, Admitted<'d>)> {
        let mut left = budget.get();
        if self.error.is_some() {
            if self.release(&mut left).is_pending() {
                return Outcome::Pending(self);
            }
            if let Some(Err(error)) = self.error {
                return Outcome::Failed(error);
            }
            return Outcome::Pending(self);
        }
        match self.try_step(&mut left) {
            Ok(None) => Outcome::Pending(self),
            Ok(Some(input)) => Outcome::Ready((self.doc, Admitted { input })),
            Err(error) => {
                self.error = Some(Err(error.into()));
                Outcome::Pending(self)
            }
        }
    }

    fn try_step(
        &mut self,
        budget: &mut usize,
    ) -> crate::Result<Option<super::super::input::Input<'d>>> {
        if self.parts.is_none() {
            let value = self.doc.field(self.field)?.ok_or(crate::Error::Invalid)?;
            self.parts = Some(value.split()?);
        }
        match &mut self.parts {
            Some(parts) => parts.step(&self.doc.input, budget),
            None => Ok(None),
        }
    }
}
