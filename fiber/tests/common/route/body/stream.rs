use core::array;
use std::{cell::Cell, fmt, rc::Rc};

use dope_test::{fibers::Yield, harness::observe::VecCellExt as _};
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
use sark_core::life::{self, audit};
use sark_fiber::fiber::{IntoStream, IterStream};
use sark_http::field::{self, Value};

use crate::common::route::body::{Chunks, OwnedReply, Trace};

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
async fn empty() -> Stream<'static, IterStream<array::IntoIter<Chunk<'d>, 0>>> {
    Yield::new().await;
    [].into_stream()
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
fn pending() -> Stream<'req, Chunks<Chunk<'d>>, 1> {
    Stream::new(Chunks::new(
        [
            Chunk::from(b"alpha"),
            Chunk::from(b"-beta"),
            Chunk::from(b"-gamma"),
        ],
        None,
    ))
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

pub struct Failure<'a> {
    pub cause: &'a str,
    pub trace: Rc<Trace>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    Ready,
    Committed,
    Terminal(life::Outcome),
    Retired,
}

pub struct Observer<'a> {
    pub events: &'a Cell<Vec<Event>>,
    pub snapshots: &'a Cell<Vec<audit::Snapshot>>,
    pub diagnostics: &'a Cell<Option<String>>,
}

impl<C: Copy, S: Copy> life::Observer<C, S> for Observer<'_> {
    fn observe(&mut self, event: life::Event<C, S>) {
        self.events.push(match event {
            life::Event::TaskPollReady(_) => Event::Ready,
            life::Event::ResponseCommitted { .. } => Event::Committed,
            life::Event::Terminal { outcome, .. } => Event::Terminal(outcome),
            life::Event::ExchangeRetired(_) => Event::Retired,
            _ => return,
        });
    }

    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E) {
        assert!(self.diagnostics.replace(Some(format!("{cause:?}"))).is_none());
    }

    fn audit(&mut self, _: C, snapshot: audit::Snapshot) {
        self.snapshots.push(snapshot);
    }
}

impl Failure<'_> {
    fn stream<'a, 'd>(
        &self,
        cause: &'a str,
    ) -> Stream<'static, Chunks<Result<Chunk<'d>, &'a str>>> {
        Stream::new(Chunks::new(
            [Ok(Chunk::from(b"before-error")), Err(cause)],
            Some(Rc::clone(&self.trace)),
        ))
    }
}

#[sark_fiber::handler(pub)]
fn failing(state: &Failure<'_>) -> Stream<'static, Chunks<Result<Chunk<'d>, &'static str>>> {
    state.stream("read-failed")
}

#[sark_fiber::handler(pub)]
async fn later<'a>(state: &Failure<'a>) -> Stream<'static, Chunks<Result<Chunk<'d>, &'a str>>> {
    Yield::new().await;
    state.stream(state.cause)
}

#[sark_fiber::handler(pub)]
fn next() -> OwnedReply {
    OwnedReply {
        status: sark_http::payload::response::status::Code::OK,
        body: b"next".to_vec(),
    }
}

sark_fiber::define_route! {
    pub StreamFailureDispatch<'a>: Failure<'a> => {
        GET "/direct" => stream(capacity = 1) failing,
        GET "/later" => async stream(capacity = 1, body = discarded) later,
        HEAD "/later" => async stream(capacity = 1, body = discarded) later,
        GET "/next" => next,
    }
}
