use core::array;

use o3::buffer::{
    resident,
    storage::{self, Shared},
};
use sark::http::payload::response::{
    dispatch::{
        Stream,
        chunk::{self, Chunk},
    },
    headers::{set, value::Item},
};
use sark_fiber::fiber::{IntoStream, IterStream};
use sark_http::field::{self, Value};

use crate::common::route::body::Chunks;

#[sark_fiber::request]
pub struct EchoReq {
    #[header("x-echo", default = "none")]
    pub echo: Shared,
    #[raw_body]
    pub payload: Shared,
}

#[sark_fiber::handler(pub)]
fn greeting() -> Stream<'req, IterStream<array::IntoIter<Chunk<'d>, 2>>, 1> {
    [Chunk::from(b"hello"), Chunk::from(b" world")]
        .into_stream()
        .headers(set::Set::from_items([Item::borrow(
            sark_fiber::header_name!("content-type"),
            field::literal!(Value::try_bytes(b"text/plain")),
        )]))
}

#[sark_fiber::handler(pub)]
fn array_handler() -> Stream<'req, IterStream<array::IntoIter<Chunk<'d>, 2>>> {
    [Chunk::from(b"hello "), Chunk::from(b"world!")].into_stream()
}

#[sark_fiber::handler(pub)]
fn echo_handler<F>(request: EchoReq, state: &State<F>) -> Output<'req, 'd>
where
    F: for<'r, 's> Fn(Result<Output<'r, 's>, Error<'s>>) -> Output<'r, 's>,
{
    (state.0)((|| {
        let chunk = Chunk::try_new(resident::Shared::from(
            Shared::try_copy(request.payload).map_err(Error::Copy)?,
        ))
        .map_err(Error::Empty)?;
        Ok([chunk]
            .into_stream()
            .headers(set::Set::from_items([Item::borrow(
                sark_fiber::header_name!("x-echo"),
                Value::try_bytes(request.echo).map_err(Error::Header)?,
            )])))
    })())
}

type Output<'req, 'd> = Stream<'req, IterStream<array::IntoIter<Chunk<'d>, 1>>, 1>;

pub struct State<F>(pub F)
where
    F: for<'r, 's> Fn(Result<Output<'r, 's>, Error<'s>>) -> Output<'r, 's>;

#[derive(Debug)]
pub enum Error<'d> {
    Copy(storage::AllocError),
    Empty(chunk::Empty<'d>),
    Header(field::Error),
}

sark_fiber::define_route! {
    pub StreamDispatch<F>: State<F>
    where F: for<'r, 's> Fn(Result<Output<'r, 's>, Error<'s>>) -> Output<'r, 's>
    => {
        GET "/stream" => stream(capacity = 32) greeting,
        GET "/arrays" => stream(capacity = 32) array_handler,
        POST "/echo" => request(header) stream(capacity = 32) echo_handler,
    }
}

#[sark_fiber::handler(pub)]
fn pending() -> Stream<'req, Chunks<'d>, 1> {
    Stream::new(Chunks::new([
        Chunk::from(b"alpha"),
        Chunk::from(b"-beta"),
        Chunk::from(b"-gamma"),
    ]))
    .headers(set::Set::from_items([Item::borrow(
        sark_fiber::header_name!("content-type"),
        field::literal!(Value::try_bytes(b"text/plain")),
    )]))
}

sark_fiber::define_route! {
    pub StreamPendingDispatch: () => {
        GET "/stream" => stream(capacity = 32) pending,
    }
}
