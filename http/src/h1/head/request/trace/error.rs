use crate::request::span::Span;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Error {
    pub first: Span,
    pub next: Span,
}
