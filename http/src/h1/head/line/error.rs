use core::mem;

use crate::{h1::target, payload::response::status, request::span};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Request {
    Syntax,
    Target(target::Error),
    Span(span::Error),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Response {
    Syntax,
    Status(status::Error),
}

const _: () = {
    assert!(mem::size_of::<Request>() == 5 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Request>>() == mem::size_of::<Request>());
    assert!(!mem::needs_drop::<Request>());
    assert!(mem::size_of::<Response>() <= 2 * mem::size_of::<u16>());
    assert!(mem::size_of::<Option<Response>>() == mem::size_of::<Response>());
    assert!(!mem::needs_drop::<Response>());
};
