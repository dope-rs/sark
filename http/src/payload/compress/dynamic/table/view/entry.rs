use crate::payload::compress::dynamic::{Field, mode, table::state};

pub(in crate::payload::compress::dynamic::table) struct Entry<'a, M: mode::Kind> {
    field: Field<'a>,
    record: &'a M::Record,
    size: state::Size,
}

impl<'a, M: mode::Kind> Entry<'a, M> {
    pub(super) const fn new(field: Field<'a>, record: &'a M::Record, size: state::Size) -> Self {
        Self {
            field,
            record,
            size,
        }
    }

    pub(in crate::payload::compress::dynamic::table) const fn field(self) -> Field<'a> {
        self.field
    }

    pub(in crate::payload::compress::dynamic::table) const fn size(&self) -> state::Size {
        self.size
    }

    pub(in crate::payload::compress::dynamic::table) const fn record(&self) -> &'a M::Record {
        self.record
    }
}
