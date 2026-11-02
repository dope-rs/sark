use std::ptr;

use o3::depot::map::compact::Insert as _;

use crate::{
    field::metadata::{block, map::record::Span},
    payload::compress::dynamic::{
        Field, Whole, hash,
        table::delta::{
            Admission,
            buffer::{Storage, insert::Insert, record},
        },
        table::state::Size,
    },
};

pub(in crate::payload::compress::dynamic::table::delta) struct Append<'a> {
    pub(super) storage: &'a mut Storage,
}

pub(in crate::payload::compress::dynamic::table::delta) struct Ready<'a, 'field> {
    pub(super) storage: &'a mut Storage,
    pub(super) plan: record::Plan<'field>,
}

impl<'a> Append<'a> {
    pub(in crate::payload::compress::dynamic::table::delta) fn try_prepare<'field>(
        self,
        probe: hash::Probe<'field>,
        size: Size,
        proof: block::Name,
    ) -> Result<Ready<'a, 'field>, Admission> {
        let head = self.storage.header();
        let field = probe.field();
        let field = Field {
            name: Span::from_slice(field.name),
            value: Span::from_slice(field.value),
            proof,
            validation: Whole::from_value(
                proof,
                block::Value::from_span(Span::from_slice(field.value)),
            ),
        };
        let plan = record::Plan::new(
            field,
            probe.digest(),
            size,
            head.used,
            head.len,
            head.bytes,
            head.entries,
        )?;
        Ok(Ready {
            storage: self.storage,
            plan,
        })
    }
}

impl Ready<'_, '_> {
    pub(in crate::payload::compress::dynamic::table::delta) fn commit(self) {
        let Self { storage, plan } = self;
        let record = plan.record();
        let field = plan.field();
        let (head, bytes) = storage.block.parts_mut();
        let pointer = bytes.as_mut_ptr().cast::<u8>();
        // SAFETY: the retained plan admitted this record and all its data in
        // the disjoint tail planes. Header alignment also aligns the record
        // plane at the tail's start; publication follows both index inserts.
        unsafe {
            pointer
                .cast::<record::Record>()
                .add(plan.ordinal())
                .write(record);
            let mut out = pointer.add(head.plan.data + record.start());
            for span in [field.name, field.value] {
                let (first, second) = span.parts();
                ptr::copy(first.as_ptr(), out, first.len());
                out = out.add(first.len());
                ptr::copy(second.as_ptr(), out, second.len());
                out = out.add(second.len());
            }
        }
        let exact = Insert::<0> {
            ready: Ready { storage, plan },
        }
        .insert();
        exact.insert();
    }
}
