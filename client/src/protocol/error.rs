use core::convert::Infallible;

use o3::{
    alloc,
    buffer::{hold, storage::range, write::consume::scope},
};
use sark_http::h1::{
    chunk,
    codec::decode::{chunks, trailer},
    framing, head, switch,
};

use crate::protocol::sequence;

#[derive(Debug)]
pub enum Error {
    Input(scope::Error),
    Range(range::Error),
    Retention(hold::Error),
    Compact(alloc::Error),
    Head(head::ParseError<Infallible>),
    HeadLimit(head::Boundary),
    Framing(framing::Error),
    Transition(switch::Error),
    UnsolicitedResponse,
    Chunk(chunks::Error),
    Trailer(chunk::Error),
    TrailerStore(trailer::StoreError),
    InformationalLimit,
    BodyLimit,
    ForbiddenContent,
    IncompleteBody,
    IncompleteChunked,
    IncompleteHead,
    Sequence(sequence::Error),
}
