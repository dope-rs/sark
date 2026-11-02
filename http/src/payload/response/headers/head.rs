use crate::payload::response::{headers, headers::set};

pub struct Template<'a> {
    fields: &'a [headers::Entry<'a>],
}

impl<'a> Template<'a> {
    pub const fn new(fields: &'a [headers::Entry<'a>]) -> Self {
        Self { fields }
    }

    fn fields(&self) -> impl Iterator<Item = headers::Entry<'a>> + '_ {
        self.fields.iter().copied()
    }
}

pub struct Head<'req, const N: usize = { set::DEFAULT_CAPACITY }> {
    template: &'req Template<'req>,
    headers: set::Set<'req, N>,
}

impl<'req, const N: usize> Head<'req, N> {
    pub fn new(template: &'req Template<'req>, headers: set::Set<'req, N>) -> Self {
        Self { template, headers }
    }

    fn fields(&self) -> impl Iterator<Item = headers::Entry<'_>> {
        self.template.fields().chain(self.headers.fields())
    }
}

impl<const N: usize> headers::Source for Head<'_, N> {
    fn emit<'field>(&'field self, sink: &mut impl headers::Sink<'field>) {
        for field in self.fields() {
            sink.entry(field);
        }
    }
}
