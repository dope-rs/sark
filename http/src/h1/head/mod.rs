//! HTTP/1 head syntax, framing, and admission.

use core::mem;

use crate::{
    h1::{
        field::{self, syntax},
        framing, switch, target,
    },
    payload::response::status,
    request::span,
};

mod limit;
pub mod line;
pub mod request;
pub mod response;
mod state;
mod tail;

pub use limit::Limit;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Boundary {
    Invalid,
    TooLarge { actual: usize, limit: usize },
    Span(span::Error),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Semantic {
    Status(status::Error),
    Field(field::Error),
    Host(target::HostError),
    Routing(target::Invalid),
    Authority(target::Error),
    Connection(field::TokenError),
    Upgrade(switch::UpgradeError),
    Admission(framing::RequestError),
    Transition(switch::Error),
    Framing(framing::Error),
}

const _: () = {
    assert!(mem::size_of::<Boundary>() == 4 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Boundary>>() == mem::size_of::<Boundary>());
    assert!(!mem::needs_drop::<Boundary>());
    assert!(mem::size_of::<Semantic>() == 6 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Semantic>>() == mem::size_of::<Semantic>());
    assert!(!mem::needs_drop::<Semantic>());
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ParseError<E> {
    Boundary(Boundary),
    Field(syntax::Error),
    Semantic(Semantic),
    Sink(E),
}

impl<E> ParseError<E> {
    pub const fn invalid() -> Self {
        Self::Boundary(Boundary::Invalid)
    }

    pub const fn too_large(actual: usize, limit: usize) -> Self {
        Self::Boundary(Boundary::TooLarge { actual, limit })
    }
}
