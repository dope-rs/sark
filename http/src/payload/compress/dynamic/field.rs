use crate::{
    field::metadata::{
        block::{self, table::Entry},
        map::record::Span,
    },
    payload::compress::dynamic::Whole,
};

pub(super) const NAME_LEN_BITS: u32 = 27;
pub(super) const NAME_LEN_MASK: u32 = (1 << NAME_LEN_BITS) - 1;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Field<'a> {
    pub(crate) name: Span<'a>,
    pub(crate) value: Span<'a>,
    pub(crate) proof: block::Name,
    pub(crate) validation: Whole,
}

impl<'a> Field<'a> {
    pub const fn name(self) -> block::Named<'a> {
        block::Named::from_scanned(self.name, self.proof)
    }

    pub const fn value(self) -> Span<'a> {
        self.value
    }

    /// Retains the entry's admission verdict, including rejection of its name.
    pub const fn valued(self) -> block::Valued<'a> {
        block::Valued::from_scanned(self.value, self.validation.validation())
    }

    pub const fn validation(self) -> Whole {
        self.validation
    }

    pub const fn data_len(self) -> usize {
        self.name.len() + self.value.len()
    }
}

impl<'a> From<Entry<'_, 'a>> for Field<'a> {
    fn from(entry: Entry<'_, 'a>) -> Self {
        let name = entry.name();
        Self {
            name: name.span(),
            value: Span::from_slice(entry.value()),
            proof: name.proof(),
            validation: Whole::valid(name.proof()),
        }
    }
}
