use std::{cell::Cell, convert::Infallible, pin::Pin};

use dope::core::driver::schedule::sequence;
use o3::buffer;
use sark_core::{body::credit, manifold::task::request::Input, route};
use sark_fiber::request::BodyLen;
use sark_h1::{
    pump,
    route::task,
    session, switch,
    wire::{
        head,
        parsing::{self, framing},
    },
};
use sark_http::{
    h1::{codec::canned, head::line::error},
    payload::{fixed, owned, response::status::Code},
    request::{Id, Seq},
};

mod drop;

pub const HEAD: &[u8] = b"GET / HTTP/1.1\r\nHost: empty\r\nX-Proof: kept\r\n\r\n";
pub const BODY: &[u8] =
    b"POST / HTTP/1.1\r\nHost: empty\r\nX-Proof: kept\r\nContent-Length: 1\r\n\r\nx";
pub const CHUNKED: &[u8] =
    b"POST / HTTP/1.1\r\nHost: empty\r\nX-Proof: kept\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n\r\n";
pub const ENCODING_BODY: &[u8] =
    b"POST / HTTP/1.1\r\nHost: empty\r\nX-Proof: kept\r\nAccept-Encoding: br\r\nAccept-Encoding: gzip\r\nContent-Length: 1\r\n\r\nx";

thread_local! {
    pub static DROPS: Cell<usize> = const { Cell::new(0) };
}

#[sark_fiber::request]
pub struct Request {}

#[sark_fiber::request]
pub struct Tracked {}

#[sark_fiber::request]
pub struct Head {}

#[sark_fiber::request]
pub struct Inspected {
    #[body_len]
    pub len: BodyLen,
    #[header("x-proof")]
    pub proof: Option<Shared>,
    #[header("accept-encoding")]
    pub encoding: Option<sark_http::field::head::AcceptEncoding>,
}

#[sark_fiber::response(raw)]
pub struct Reply<'d> {
    pub status: Code,
    body: fixed::Body<'d>,
}

#[sark_fiber::handler(pub)]
async fn inspect(_: Inspected) -> Reply<'d> {
    Reply {
        status: Code::OK,
        body: fixed::Body::new(b"ok"),
    }
}

#[sark_fiber::handler(pub)]
async fn plain(_: Request, calls: &Cell<usize>) -> Reply<'d> {
    calls.set(calls.get() + 1);
    Reply {
        status: Code::OK,
        body: fixed::Body::new(b"ok"),
    }
}

#[sark_fiber::handler(pub)]
async fn tracked(request: Tracked, calls: &Cell<usize>) -> Reply<'d> {
    calls.set(calls.get() + 1);
    drop(request);
    Reply {
        status: Code::OK,
        body: fixed::Body::new(b"ok"),
    }
}

#[sark_fiber::handler(pub)]
async fn built(
    request: crate::common::route::chunked::apps::JsonReq,
    calls: &Cell<usize>,
) -> Reply<'d> {
    calls.set(calls.get() + 1);
    Reply {
        status: if request.body.value.as_slice() == b"ok" {
            Code::OK
        } else {
            Code::BAD_REQUEST
        },
        body: fixed::Body::new(b"ok"),
    }
}

type Connection<'d, R> =
    session::Conn<'d, (), task::Pending<'d, R>, switch::Disabled, task::Ready<'d, R>>;

type Outcome<'d, R, Rejected = Infallible> =
    session::RouteOutcome<'d, task::Pending<'d, R>, Infallible, Infallible, Rejected>;

pub struct Carrier<'a, 'd, R: route::Spec<Request: route::Own>> {
    id: Id<'d>,
    input: &'a mut Option<Input<'d, R>>,
    consumed: usize,
    error: Option<Error>,
}

#[derive(Debug)]
pub enum Error {
    Line(error::Request),
    IncompleteLine,
    IncompleteHead,
    Identity(sequence::Exhausted),
    Ingress(pump::Reject),
    Input { request: u8 },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl<'d, R: route::Spec<Request: route::Own<RawParams = (), Mode = route::Streaming>>>
    Carrier<'_, 'd, R>
{
    pub fn try_dispatch(
        id: Id<'d>,
        wire: &[u8],
        accept: bool,
    ) -> Result<(Option<Input<'d, R>>, Outcome<'d, R>, usize)> {
        use sark_h1::wire::parsing::raw;

        let parsed = parsing::Head::parse(sark_h1::Input::Borrowed(wire), u8::MAX)
            .map_err(Error::Line)?
            .ok_or(Error::IncompleteLine)?;
        let head = match framing::Head::<R>::parse(raw::Bound::empty(&parsed)) {
            Ok(head) => head.ok_or(Error::IncompleteHead)?,
            Err(error) => {
                return Ok((
                    None,
                    session::RouteOutcome::Outcome(error.into_consume()),
                    0,
                ));
            }
        };
        let mut conn: session::Conn = session::Conn::default();
        let mut input = None;
        let mut attempts = 0;
        let outcome = task::Endpoint::new(&mut conn).dispatch::<R, _, _, _>(
            id,
            session::Permit::new(),
            head,
            |request, consumed, close, _, _: &mut ()| {
                attempts += 1;
                if !accept {
                    return session::Outcome::Close(canned::SERVICE_UNAVAILABLE);
                }
                input = Some(request);
                session::Outcome::<Infallible>::Park { consumed, close }
            },
            |pending, parts, proof| pending.try_discard(parts, proof),
            credit::Disabled::new(),
        );
        Ok((input, outcome, attempts))
    }

    pub fn try_receive(
        sequence: &Seq<'d>,
        first: &[u8],
        last: &[u8],
    ) -> Result<(Input<'d, R>, usize, Input<'d, R>, usize)> {
        let mut input = None;
        let mut carrier = Carrier {
            id: sequence.try_next().map_err(Error::Identity)?,
            input: &mut input,
            consumed: 0,
            error: None,
        };
        let mut conn = Connection::default();
        for part in [first, last].into_iter().filter(|part| !part.is_empty()) {
            let cycle = pump::Ingress::drive(
                &mut conn,
                Pin::new(&mut carrier),
                part,
                &mut [0; 2048],
                head::Connection::KeepAlive,
            );
            if let Some(error) = carrier.error.take() {
                return Err(error);
            }
            cycle.state.map_err(Error::Ingress)?;
        }
        let consumed = carrier.consumed;
        let input = carrier.input.take().ok_or(Error::Input { request: 0 })?;
        carrier.id = sequence.try_next().map_err(Error::Identity)?;
        let cycle = pump::Ingress::drive(
            &mut conn,
            Pin::new(&mut carrier),
            &[],
            &mut [0; 2048],
            head::Connection::KeepAlive,
        );
        if let Some(error) = carrier.error.take() {
            return Err(error);
        }
        cycle.state.map_err(Error::Ingress)?;
        Ok((
            input,
            consumed,
            carrier.input.take().ok_or(Error::Input { request: 1 })?,
            carrier.consumed,
        ))
    }

    fn admit(
        &mut self,
        input: Input<'d, R>,
        consumed: usize,
        close: head::Connection,
    ) -> session::Outcome<'d> {
        *self.input = Some(input);
        self.consumed = consumed;
        session::Outcome::Park { consumed, close }
    }
}

impl<'d, R> session::Dispatch<'d, (), task::Pending<'d, R>> for Carrier<'_, 'd, R>
where
    R: route::Spec<Request: route::Own<RawParams = (), Mode = route::Streaming>>,
{
    type Ready = task::Ready<'d, R>;
    type Head = Infallible;

    fn try_consume(
        self: Pin<&mut Self>,
        permit: session::Permit,
        input: sark_h1::Input<'_>,
        _: &mut [u8],
        conn: &mut Connection<'d, R>,
    ) -> Outcome<'d, R, parsing::Rejected<'d>> {
        use sark_h1::wire::parsing::raw;

        let this = self.get_mut();
        let parsed = match parsing::Head::parse(input, u8::MAX)
            .map_err(Error::Line)
            .and_then(|head| head.ok_or(Error::IncompleteLine))
        {
            Ok(parsed) => parsed,
            Err(error) => {
                this.error.get_or_insert(error);
                return session::Outcome::Close(canned::BAD_REQUEST).into();
            }
        };
        let head = match framing::Head::<R>::parse(raw::Bound::empty(&parsed)) {
            Ok(Some(head)) => head,
            Ok(None) => {
                this.error.get_or_insert(Error::IncompleteHead);
                return session::Outcome::Close(canned::BAD_REQUEST).into();
            }
            Err(error) => return error.into_consume().into(),
        };
        let id = this.id;
        let admit = |input, consumed, close, _, _: &mut ()| this.admit(input, consumed, close);
        task::Endpoint::new(conn)
            .dispatch::<R, _, _, _>(
                id,
                permit,
                head,
                admit,
                |pending, parts, proof| pending.try_discard(parts, proof),
                credit::Disabled::new(),
            )
            .with_head()
    }

    fn consume_line(
        self: Pin<&mut Self>,
        permit: session::Permit,
        source: parsing::Source,
        write: &mut [u8],
        conn: &mut Connection<'d, R>,
    ) -> Outcome<'d, R, parsing::Rejected<'d>> {
        let source = source.freeze();
        self.try_consume(
            permit,
            sark_h1::Input::Buffered(buffer::Complete::view(&source)),
            write,
            conn,
        )
    }

    fn resume_head(
        self: Pin<&mut Self>,
        pending: Self::Head,
        _: session::Permit,
        _: sark_h1::Input<'_>,
        _: &mut [u8],
        _: &mut Connection<'d, R>,
    ) -> Outcome<'d, R, parsing::Rejected<'d>> {
        match pending {}
    }

    fn body_data(
        self: Pin<&mut Self>,
        pending: task::Pending<'d, R>,
        body: owned::Segments<'d>,
        _: &mut (),
    ) -> Result<task::Pending<'d, R>, fixed::Body<'d>> {
        pending.try_discard(body, credit::Disabled::new())
    }

    fn has_body_work(pending: &task::Pending<'d, R>) -> bool {
        pending.has_work()
    }

    fn resume_pending(
        self: Pin<&mut Self>,
        pending: task::Pending<'d, R>,
        _: &mut (),
    ) -> Result<task::Pending<'d, R>, fixed::Body<'d>> {
        pending.resume(credit::Disabled::new())
    }

    fn finish_body(
        self: Pin<&mut Self>,
        pending: task::Pending<'d, R>,
        complete: session::Complete,
        permit: session::Permit,
        _: &mut [u8],
        runtime: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready> {
        let this = self.get_mut();
        let admit = |input, consumed, close, _, _: &mut ()| this.admit(input, consumed, close);
        pending.finish(complete, permit, runtime, admit, credit::Disabled::new())
    }

    fn resume_body(
        self: Pin<&mut Self>,
        ready: Self::Ready,
        permit: session::Permit,
        _: &mut [u8],
        runtime: &mut (),
    ) -> session::BodyOutcome<'d, Self::Ready> {
        let this = self.get_mut();
        let admit = |input, consumed, close, _, _: &mut ()| this.admit(input, consumed, close);
        ready.resume(permit, runtime, admit, credit::Disabled::new())
    }
}
