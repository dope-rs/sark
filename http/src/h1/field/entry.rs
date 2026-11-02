use crate::{
    field::{head, scan::Match},
    request::span,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Entry<'owner> {
    name: &'owner [u8],
    value: &'owner [u8],
    name_span: span::Span,
    value_span: span::Span,
    line_span: span::Span,
}

impl<'owner> Entry<'owner> {
    /// Splits one CRLF-terminated field without validating its name or value.
    /// Admitted sections retain their grammar proof; arbitrary input returns `None`
    /// when a required delimiter is missing. The value retains its original OWS.
    pub fn split(bytes: &[u8]) -> Option<(&[u8], &[u8], &[u8])> {
        let end = Match::find(b'\r', bytes)?;
        let rest = end.suffix().strip_prefix(b"\n")?;
        let colon = Match::find(b':', end.prefix())?;
        Some((colon.prefix(), colon.suffix(), rest))
    }

    pub(super) fn new(
        name: span::Retained<'owner>,
        value: span::Retained<'owner>,
        line: span::Span,
    ) -> Self {
        Self {
            name: name.bytes(),
            value: value.bytes(),
            name_span: name.span(),
            value_span: value.span(),
            line_span: line,
        }
    }

    pub(crate) fn kind(&self) -> head::Name {
        match head::Name::from_h1(self.name()) {
            Some(name) => name,
            None => head::Name::Regular,
        }
    }

    pub fn name(&self) -> &'owner [u8] {
        self.name
    }

    pub fn value(&self) -> &'owner [u8] {
        self.value
    }

    pub const fn name_span(&self) -> span::Span {
        self.name_span
    }

    pub const fn value_span(&self) -> span::Span {
        self.value_span
    }

    pub const fn line_span(&self) -> span::Span {
        self.line_span
    }
}

const _: () = assert!(core::mem::size_of::<Entry<'_>>() == 56);
