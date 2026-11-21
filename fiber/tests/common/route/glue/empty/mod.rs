use std::{cell::Cell, convert::Infallible, ops::ControlFlow, pin::Pin};

use dope::core::driver::schedule::sequence;
use sark_core::{body::credit, manifold::task::request::Input, route};
use sark_fiber::request::BodyLen;
use sark_h1::{
    pump,
    route::task,
    session, switch,
    wire::{
        parsing::{self, framing},
        response::Context,
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

type Outcome<'d, R> = session::RouteOutcome<'d, task::Pending<'d, R>>;

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
        let head = match framing::Head::<R>::parse::<false>(raw::Bound::empty(&parsed)) {
            Ok(head) => head.ok_or(Error::IncompleteHead)?,
            Err(error) => {
                return Ok((
                    None,
                    session::RouteOutcome::Outcome(error.into_rejection().into()),
                    0,
                ));
            }
        };
        let mut conn: session::Conn = session::Conn::default();
        let mut input = None;
        let mut attempts = 0;
        let outcome = conn.dispatch_task::<R, _, _, _>(
            id,
            head,
            |request, consumed, response, _: &mut ()| {
                attempts += 1;
                if !accept {
                    return session::Outcome::Close(canned::SERVICE_UNAVAILABLE);
                }
                input = Some(request);
                session::Outcome::park(consumed, response)
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
            carrier.receive(&mut conn, part)?;
        }
        let consumed = carrier.consumed;
        let input = carrier.input.take().ok_or(Error::Input { request: 0 })?;
        carrier.id = sequence.try_next().map_err(Error::Identity)?;
        carrier.receive(&mut conn, &[])?;
        Ok((
            input,
            consumed,
            carrier.input.take().ok_or(Error::Input { request: 1 })?,
            carrier.consumed,
        ))
    }

    fn receive(&mut self, conn: &mut Connection<'d, R>, bytes: &[u8]) -> Result<()> {
        let result = conn.drive(
            Pin::new(&mut *self),
            bytes,
            o3::buffer::write::Slice::new(&mut [0; 2048]).reborrow(),
            &mut pump::Tail::Empty,
        );
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        result.map(|_| ()).map_err(Error::Ingress)
    }

    fn admit(
        &mut self,
        input: Input<'d, R>,
        consumed: usize,
        response: Context,
    ) -> session::Outcome<'d> {
        *self.input = Some(input);
        self.consumed = consumed;
        session::Outcome::park(consumed, response)
    }
}

impl<'d, R> session::Dispatch<'d, (), task::Pending<'d, R>> for Carrier<'_, 'd, R>
where
    R: route::Spec<Request: route::Own<RawParams = (), Mode = route::Streaming>>,
{
    type Ready = task::Ready<'d, R>;
    type Head = Infallible;

    fn try_consume(
        input: sark_h1::Input<'_>,
        turn: &mut session::Turn<'_, '_, 'd, (), task::Pending<'d, R>, switch::Disabled, Self>,
        prior: usize,
    ) -> session::Flow {
        use sark_h1::wire::parsing::raw;

        let this = turn.router.as_mut().get_mut();
        let parsed = match parsing::Head::parse(input, u8::MAX)
            .map_err(Error::Line)
            .and_then(|head| head.ok_or(Error::IncompleteLine))
        {
            Ok(parsed) => parsed,
            Err(error) => {
                this.error.get_or_insert(error);
                return turn.close(canned::BAD_REQUEST);
            }
        };
        let head = match framing::Head::<R>::parse::<false>(raw::Bound::empty(&parsed)) {
            Ok(Some(head)) => head,
            Ok(None) => {
                this.error.get_or_insert(Error::IncompleteHead);
                return turn.close(canned::BAD_REQUEST);
            }
            Err(error) => return turn.rejected(error.into_rejection()),
        };
        let id = this.id;
        let admit = |input, consumed, response, _: &mut ()| this.admit(input, consumed, response);
        let result = match turn.conn.dispatch_task::<R, _, _, _>(
            id,
            head,
            admit,
            |pending, parts, proof| pending.try_discard(parts, proof),
            credit::Disabled::new(),
        ) {
            session::RouteOutcome::Outcome(outcome) => outcome,
            session::RouteOutcome::AwaitBody {
                state,
                expectation,
                pending,
            } => return turn.await_body(state, expectation, pending, prior),
        };
        turn.apply(result, prior)
    }

    fn resume_head(
        pending: Self::Head,
        _: &mut session::Turn<'_, '_, 'd, (), task::Pending<'d, R>, switch::Disabled, Self>,
    ) -> session::Flow {
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
        pending: task::Pending<'d, R>,
        complete: session::Complete,
        turn: &mut session::Turn<'_, '_, 'd, (), task::Pending<'d, R>, switch::Disabled, Self>,
    ) -> session::Flow {
        let used = complete.consumed;
        let this = turn.router.as_mut().get_mut();
        let result = pending.finish(
            complete,
            &mut turn.conn.async_state,
            |input, consumed, response, _| this.admit(input, consumed, response),
            credit::Disabled::new(),
        );
        match result {
            ControlFlow::Continue(action) => turn.apply(action, 0),
            ControlFlow::Break(ready) => turn.await_ready(ready, used),
        }
    }

    fn resume_body(
        ready: Self::Ready,
        turn: &mut session::Turn<'_, '_, 'd, (), task::Pending<'d, R>, switch::Disabled, Self>,
    ) -> session::Flow {
        let this = turn.router.as_mut().get_mut();
        let result = ready.resume(
            &mut turn.conn.async_state,
            |input, consumed, response, _| this.admit(input, consumed, response),
            credit::Disabled::new(),
        );
        match result {
            ControlFlow::Continue(action) => turn.apply(action, 0),
            ControlFlow::Break(ready) => turn.await_ready(ready, 0),
        }
    }
}
