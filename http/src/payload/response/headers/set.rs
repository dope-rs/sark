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

    pub fn from_optional(items: [Option<value::Item<'req>>; N]) -> Self {
        let mut items = items.into_iter().flatten();
        let mut len = 0;
        let entries = std::array::from_fn(|_| match items.next() {
            Some(item) => {
                len += 1;
                item
            }
            None => value::Item::placeholder(),
        });
        Self { entries, len }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl<const N: usize> headers::Source for Set<'_, N> {
    fn emit<'field>(&'field self, sink: &mut impl headers::Sink<'field>) {
        for item in &self.entries[..self.len.min(N)] {
            sink.entry(headers::Entry::new(item.name, item.value.borrow()));
        }
    }
}
