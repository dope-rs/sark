use crate::h1::field;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Limit {
    bytes: usize,
    fields: field::Limit,
}

impl Limit {
    pub const DEFAULT: Self = Self::new(128, 8 * 1024);
    pub const RESPONSE: Self = Self::new(100, usize::MAX);

    pub const fn new(fields: usize, line: usize) -> Self {
        Self {
            bytes: usize::MAX,
            fields: field::Limit::new(fields, line),
        }
    }

    pub const fn with_bytes(self, bytes: usize) -> Self {
        Self { bytes, ..self }
    }

    pub(super) const fn bytes(self) -> usize {
        self.bytes
    }

    pub(super) const fn fields(self) -> field::Limit {
        self.fields
    }
}
