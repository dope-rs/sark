use crate::{
    field,
    payload::response::{headers, headers::set},
};

pub struct Template<'a> {
    fields: &'a [headers::Entry<'a>],
}

impl<'a> Template<'a> {
    pub const fn new(fields: &'a [headers::Entry<'a>]) -> Self {
        Self { fields }
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
}

impl<const N: usize> field::Source<headers::Application> for Head<'_, N> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field, headers::Application>) {
        field::Source::emit(self.template.fields, sink);
        field::Source::emit(&self.headers, sink);
    }
}
