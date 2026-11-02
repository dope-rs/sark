use crate::{
    h1::{field, head::request::trace},
    request::span::Span,
};

#[derive(Default)]
pub struct Fields {
    pub(in crate::h1::head::request) context: trace::Context,
    pub(in crate::h1::head::request) line: Option<Span>,
}

impl Fields {
    pub fn context(&self) -> trace::Context {
        self.context
    }

    pub fn observe(&mut self, field: field::Entry<'_>) -> Result<(), trace::Error> {
        if !field.name().eq_ignore_ascii_case(b"traceparent") {
            return Ok(());
        }
        if let Some(first) = self.line {
            return Err(trace::Error {
                first,
                next: field.line_span(),
            });
        }
        if let Some(context) = trace::Context::parse(field.value()) {
            self.context = context;
        }
        self.line = Some(field.line_span());
        Ok(())
    }
}
