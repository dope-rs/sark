use std::cell::Cell;

use sark_core::{body::credit, route};
use sark_fiber::request::BodyLen;
use sark_h1::{
    route::task,
    session,
    wire::parsing::{self, framing, raw},
};
use sark_http::{
    field::head::AcceptEncoding,
    h1::{codec::canned, head::line::error},
    payload::{fixed, response::status::Code},
    request::Id,
};

mod drop;

pub const HEAD: &[u8] = b"GET / HTTP/1.1\r\nHost: empty\r\nX-Proof: kept\r\n\r\n";
pub const BODY: &[u8] =
    b"POST /inspect HTTP/1.1\r\nHost: empty\r\nX-Proof: kept\r\nContent-Length: 1\r\n\r\nx";
pub const CHUNKED: &[u8] =
    b"POST /inspect HTTP/1.1\r\nHost: empty\r\nX-Proof: kept\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n\r\n";
pub const ENCODING_BODY: &[u8] =
    b"POST /inspect HTTP/1.1\r\nHost: empty\r\nX-Proof: kept\r\nAccept-Encoding: br\r\nAccept-Encoding: gzip\r\nContent-Length: 1\r\n\r\nx";

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
    pub encoding: Option<AcceptEncoding>,
}

#[sark_fiber::response(raw)]
pub struct Reply<'d> {
    pub status: Code,
    body: fixed::Body<'d>,
}

#[sark_fiber::handler(pub)]
async fn inspect(request: Inspected) -> Reply<'d> {
    let proof = request.proof.as_ref().map(|value| value.as_slice());
    let body: &[u8] = match (proof, request.len.len(), request.encoding) {
        (Some(b"kept"), 0, None) => b"empty",
        (Some(b"kept"), 1, None) => b"body",
        (Some(b"kept"), 1, Some(encoding))
            if Ok(encoding)
                == AcceptEncoding::try_parse(crate::common::h1::fields::ENCODING_VALUE) =>
        {
            b"encoded"
        }
        _ => b"invalid",
    };
    Reply {
        status: Code::OK,
        body: fixed::Body::new(body),
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

#[derive(Debug)]
pub enum Error {
    Line(error::Request),
    IncompleteLine,
    IncompleteHead,
}

pub fn try_dispatch<'d, R>(
    id: Id<'d>,
    wire: &[u8],
    accept: bool,
) -> Result<
    (
        Option<<R::Request as route::Own>::Input<'d>>,
        session::RouteOutcome<'d, task::Pending<'d, R>>,
        usize,
    ),
    Error,
>
where
    R: route::Spec<Request: route::Own<RawParams = (), Mode = route::Streaming>>,
{
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
