use sark_http::{field::metadata::integer, payload::compress::dynamic::table::delta};

use crate::qpack::{self, encoder, ledger};

pub(in crate::qpack::encoder) struct Query<'a, 'table, 'field> {
    base: u64,
    next: &'a mut u64,
    floor: &'a mut u64,
    wire_len: &'a mut usize,
    inner: delta::Query<'a, 'field, delta::Borrow<'table>>,
}

impl<'a, 'table, 'field> Query<'a, 'table, 'field> {
    pub(super) const fn new(
        base: u64,
        next: &'a mut u64,
        floor: &'a mut u64,
        wire_len: &'a mut usize,
        inner: delta::Query<'a, 'field, delta::Borrow<'table>>,
    ) -> Self {
        Self {
            base,
            next,
            floor,
            wire_len,
            inner,
        }
    }

    pub(in crate::qpack::encoder) fn exact(
        &self,
        permit: &mut ledger::Admission<'_>,
    ) -> Option<qpack::Reference> {
        permit.select(|permit| {
            self.inner.exact(|position| {
                let index = super::Delta::reference(self.base, position)?;
                permit.accept(index)
            })
        })
    }

    pub(in crate::qpack::encoder) fn name(
        &self,
        permit: &mut ledger::Admission<'_>,
    ) -> Option<qpack::Reference> {
        permit.select(|permit| {
            self.inner.name(|position| {
                let index = super::Delta::reference(self.base, position)?;
                permit.accept(index)
            })
        })
    }

    pub(in crate::qpack::encoder) fn protect(&mut self, index: qpack::Reference) {
        *self.floor = (*self.floor).min(index.absolute());
    }

    pub(in crate::qpack::encoder) fn try_insert(&mut self) -> Result<(), encoder::Error> {
        let field = self.inner.field();
        let Some(index) = qpack::Reference::new(*self.next) else {
            return Ok(());
        };
        let name = field.name.len() as u64;
        let value = field.value.len() as u64;
        let Some(wire) = integer::PrefixedInt::<5>::new(name)
            .encoded_len()
            .get()
            .checked_add(field.name.len())
            .and_then(|len| {
                len.checked_add(integer::PrefixedInt::<7>::new(value).encoded_len().get())
            })
            .and_then(|len| len.checked_add(field.value.len()))
            .and_then(|len| self.wire_len.checked_add(len))
        else {
            return Ok(());
        };
        let base = self.base;
        let floor = *self.floor;
        match self.inner.try_admit(|position| {
            let index = super::Delta::reference(base, position);
            index.is_some_and(|index| index.absolute() < floor)
        }) {
            Ok(_) => {
                *self.next = index.required().get();
                *self.wire_len = wire;
            }
            Err(delta::Error::Fault(error)) => return Err(encoder::Error::Delta(error)),
            Err(
                delta::Error::Length { .. }
                | delta::Error::Capacity { .. }
                | delta::Error::Name { .. }
                | delta::Error::Value { .. }
                | delta::Error::Entries { .. }
                | delta::Error::Bytes { .. }
                | delta::Error::Eviction(_),
            ) => {}
        }
        Ok(())
    }
}
