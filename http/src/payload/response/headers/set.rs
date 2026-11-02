use crate::payload::response::headers::{self, value};

pub const DEFAULT_CAPACITY: usize = 4;
pub struct Set<'req, const N: usize = DEFAULT_CAPACITY> {
    entries: [value::Item<'req>; N],
    len: usize,
}

impl<'req, const N: usize> Default for Set<'req, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'req, const N: usize> Set<'req, N> {
    pub fn new() -> Self {
        Self {
            entries: [const { value::Item::placeholder() }; N],
            len: 0,
        }
    }

    pub fn from_items(items: [value::Item<'req>; N]) -> Self {
        Self {
            entries: items,
            len: N,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn fields(&self) -> impl ExactSizeIterator<Item = headers::Entry<'_>> + '_ {
        self.entries[..self.len.min(N)]
            .iter()
            .map(|item| headers::Entry::new(item.name, item.value.borrow()))
    }
}

impl<const N: usize> headers::Source for Set<'_, N> {
    fn emit<'field>(&'field self, sink: &mut impl headers::Sink<'field>) {
        for field in self.fields() {
            sink.entry(field);
        }
    }
}
