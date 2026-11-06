//! Zero-copy HTTP/2 response encoding and retained body handoff.
use std::convert::Infallible;

use o3::buffer::{
    bytes::scope,
    hold::{
        self,
        evidence::{Mode, Off, On},
    },
    resident,
};
use sark_core::{decode, manifold::stream::Boundary};
use sark_http::{
    field::{Response, metadata::section::rules},
    owner::handoff::{self, stage},
    payload::{
        fixed,
        response::{Body, Content, bounded, dispatch, exact, headers, status::Code},
    },
    request::{Id, body},
};

use crate::{
    conn::{self, event, machine},
    stream,
};

mod detached;
mod head;
mod pending;
mod prepared;
mod receipt;
mod status;

pub(in crate::server) use detached::Detached;
#[doc(hidden)]
pub use head::Head;
pub use pending::Pending;
pub use prepared::Prepared;
#[doc(hidden)]
pub use receipt::Receipt;
#[doc(hidden)]
pub use status::Status;

pub(in crate::server) trait Handoff: Mode + Sized {
    fn resolve<'req, 'd: 'req, B: stage::Body<'req, 'd>>(
        body: handoff::Plan<'req, 'd, Infallible, B>,
    ) -> Result<Receipt<'d>, conn::Error>;

    fn detach<'req, 'd: 'req, B: stage::Body<'req, 'd>>(
        body: handoff::Plan<'req, 'd, Infallible, B>,
    ) -> Result<handoff::Pending<'d, Self, B::Deferred>, conn::Error>
    where
        B::Deferred: stage::Deferred<'d>;

    fn finish<'d, D: stage::Deferred<'d>>(
        body: handoff::Pending<'d, Self, D>,
        request: D::Owner,
    ) -> Result<Receipt<'d>, conn::Error>;
}

impl Handoff for Off {
    fn resolve<'req, 'd: 'req, B: stage::Body<'req, 'd>>(
        body: handoff::Plan<'req, 'd, Infallible, B>,
    ) -> Result<Receipt<'d>, conn::Error> {
        let body = body.finish().map_err(conn::Error::Handoff)?;
        Receipt::from_ready(body, hold::Charge::default())
    }

    fn detach<'req, 'd: 'req, B: stage::Body<'req, 'd>>(
        body: handoff::Plan<'req, 'd, Infallible, B>,
    ) -> Result<handoff::Pending<'d, Self, B::Deferred>, conn::Error>
    where
        B::Deferred: stage::Deferred<'d>,
    {
        Ok(body.detach())
    }

    fn finish<'d, D: stage::Deferred<'d>>(
        body: handoff::Pending<'d, Self, D>,
        request: D::Owner,
    ) -> Result<Receipt<'d>, conn::Error> {
        let body = body.finish(request);
        Receipt::from_ready(body, hold::Charge::default())
    }
}

impl Handoff for On {
    fn resolve<'req, 'd: 'req, B: stage::Body<'req, 'd>>(
        body: handoff::Plan<'req, 'd, Infallible, B>,
    ) -> Result<Receipt<'d>, conn::Error> {
        let (body, linked) = body.try_finish().map_err(conn::Error::Handoff)?;
        Receipt::from_ready(body, linked)
    }

    fn detach<'req, 'd: 'req, B: stage::Body<'req, 'd>>(
        body: handoff::Plan<'req, 'd, Infallible, B>,
    ) -> Result<handoff::Pending<'d, Self, B::Deferred>, conn::Error>
    where
        B::Deferred: stage::Deferred<'d>,
    {
        body.try_detach().map_err(conn::Error::Handoff)
    }

    fn finish<'d, D: stage::Deferred<'d>>(
        body: handoff::Pending<'d, Self, D>,
        request: D::Owner,
    ) -> Result<Receipt<'d>, conn::Error> {
        let (body, linked) = body.finish(request);
        Receipt::from_ready(body, linked)
    }
}

#[doc(hidden)]
pub struct Encoder<
    'a,
    'req,
    'd,
    F: rules::Policy,
    E: event::Evidence = event::Retirement,
    Q: scope::Source = (),
    B = body::Retained<'req, 'd>,
> {
    connection: &'a mut machine::Connection<
        crate::ServerRole,
        F,
        resident::Shared<'d>,
        rules::RejectPush,
        E,
    >,
    stream: stream::Key,
    body: handoff::Slot<'req, 'd, Q, B>,
}

impl<'a, 'd, F: rules::Policy, E: event::Evidence> Encoder<'a, 'd, 'd, F, E> {
    pub(crate) fn task(
        conn: &'a mut machine::Connection<
            crate::ServerRole,
            F,
            resident::Shared<'d>,
            rules::RejectPush,
            E,
        >,
        stream: stream::Key,
        id: Id<'d>,
    ) -> Self {
        Self {
            connection: conn,
            stream,
            body: handoff::Slot::task(id),
        }
    }
}

impl<'a, 'req, 'd, F: rules::Policy, E: event::Evidence> Encoder<'a, 'req, 'd, F, E> {
    #[doc(hidden)]
    pub fn new(
        conn: &'a mut machine::Connection<
            crate::ServerRole,
            F,
            resident::Shared<'d>,
            rules::RejectPush,
            E,
        >,
        stream: stream::Key,
        request: body::Ref<'req, 'd>,
    ) -> Self {
        Self {
            connection: conn,
            stream,
            body: handoff::Slot::new(request),
        }
    }
}

impl<'a, 'req, 'd, F: rules::Policy, E: event::Evidence, B> Encoder<'a, 'req, 'd, F, E, (), B> {
    pub(in crate::server) fn selected(
        conn: &'a mut machine::Connection<
            crate::ServerRole,
            F,
            resident::Shared<'d>,
            rules::RejectPush,
            E,
        >,
        stream: stream::Key,
        body: handoff::Slot<'req, 'd, (), B>,
    ) -> Self {
        Self {
            connection: conn,
            stream,
            body,
        }
    }
}

impl<
    'a,
    'req,
    'd: 'req,
    F: rules::Policy,
    E: event::Evidence,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
> Encoder<'a, 'req, 'd, F, E, Q, O>
{
    fn omit(&mut self, status: Code) -> Result<bool, (conn::Error, stream::Key)> {
        if status.is_info() {
            return Err((conn::Error::Informational(status), self.stream));
        }
        let method = self
            .connection
            .streams()
            .headers()
            .method(self.stream)
            .map_err(|error| (error, self.stream))?;
        Ok(matches!(
            Content::new(method, status.as_u16()),
            Content::Forbidden | Content::Suppressed | Content::Empty
        ))
    }

    fn empty<H: headers::Source>(
        self,
        status: Code,
        headers: H,
    ) -> Result<<Self as dispatch::Accept<'d>>::Output, <Self as dispatch::Accept<'d>>::Error> {
        self.prepare(status, headers, fixed::Body::new(&[]), Boundary::End)
    }

    fn prepare<H, B>(
        self,
        status: Code,
        headers: H,
        value: B,
        end: Boundary,
    ) -> Result<<Self as dispatch::Accept<'d>>::Output, <Self as dispatch::Accept<'d>>::Error>
    where
        H: headers::Source,
        B: Body<'d>,
    {
        let boundary = if value.is_empty() {
            end
        } else {
            Boundary::More
        };
        let Self {
            connection: conn,
            stream,
            body,
        } = self;
        let fields = Response::new(status, headers::Fields::new(headers));
        conn.streams()
            .headers()
            .prepare(stream, fields, boundary)
            .map_err(|error| (error, stream))?;
        let staged = head::Staged {
            connection: conn,
            stream,
        };
        let body = Body::try_resolve(value, &body)
            .map_err(|error| (conn::Error::Response(error), stream))?;
        Ok(Prepared { staged, body })
    }
}

impl<
    'a,
    'req,
    'd: 'req,
    F: rules::Policy,
    E: event::Evidence,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
> dispatch::Accept<'d> for Encoder<'a, 'req, 'd, F, E, Q, O>
{
    type Output = Prepared<'a, 'req, 'd, F, E, Q::Selection, O>;
    type Error = (conn::Error, stream::Key);

    fn fixed<H, B>(mut self, status: Code, headers: H, body: B) -> Result<Self::Output, Self::Error>
    where
        H: headers::Source,
        B: Body<'d>,
    {
        if self.omit(status)? {
            return self.empty(status, headers);
        }
        self.prepare(status, headers, body, Boundary::End)
    }

    fn exact<H, B>(mut self, status: Code, headers: H, body: B) -> Result<Self::Output, Self::Error>
    where
        H: headers::Source,
        B: exact::TryBody,
    {
        if self.omit(status)? {
            return self.empty(status, headers);
        }
        let plan = exact::Plan::try_new(body)
            .map_err(|rejected| (conn::Error::Size(rejected.discard()), self.stream))?;
        let body = plan
            .into_shared()
            .map_err(|error| (conn::Error::Build(error), self.stream))?;
        self.prepare(status, headers, body, Boundary::End)
    }

    fn bounded<H, B>(
        mut self,
        status: Code,
        headers: H,
        body: B,
    ) -> Result<Self::Output, Self::Error>
    where
        H: headers::Source,
        B: bounded::Body,
    {
        if self.omit(status)? {
            return self.empty(status, headers);
        }
        let body = bounded::Plan::new(body)
            .into_shared()
            .map_err(|error| (conn::Error::Build(error), self.stream))?;
        self.prepare(status, headers, body, Boundary::End)
    }
}

impl<
    'a,
    'req,
    'd: 'req,
    F: rules::Policy,
    E: event::Evidence,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
> dispatch::Sink<'d> for Encoder<'a, 'req, 'd, F, E, Q, O>
{
    type Stream<S> = (<Self as dispatch::Accept<'d>>::Output, Option<S>);

    fn stream<H: headers::Source, S>(
        mut self,
        status: Code,
        headers: H,
        stream: S,
    ) -> Result<Self::Stream<S>, Self::Error> {
        if self.omit(status)? {
            return self.empty(status, headers).map(|head| (head, None));
        }
        self.prepare(status, headers, fixed::Body::new(&[]), Boundary::More)
            .map(|head| (head, Some(stream)))
    }
}

impl<'a, 'req, 'd: 'req, F: rules::Policy, E: event::Evidence, O: stage::Body<'req, 'd>>
    decode::Encode<'req, 'd> for Encoder<'a, 'req, 'd, F, E, (), O>
{
    type Error = conn::Error;
    type Prepared = Prepared<'a, 'req, 'd, F, E, Infallible, O>;
    type Pending<'id> = Prepared<'a, 'req, 'd, F, E, scope::Range<'id>, O>;
    type Head<'h, 'id> = Encoder<'a, 'req, 'd, F, E, scope::View<'h, 'id>, O>;

    fn begin_head<'h, 'id>(self, head: scope::View<'h, 'id>) -> Self::Head<'h, 'id> {
        Encoder {
            connection: self.connection,
            stream: self.stream,
            body: self.body.with_head(head),
        }
    }
}

impl<'a, 'req, 'd: 'req, F: rules::Policy, E: event::Evidence, O: stage::Body<'req, 'd>>
    decode::Streaming<'req, 'd> for Encoder<'a, 'req, 'd, F, E, (), O>
{
    fn split_stream<S>(output: Self::Stream<S>) -> (Self::Prepared, Option<S>) {
        output
    }
}

impl<'a, 'req, 'd, F: rules::Policy, E: event::Evidence, Q: scope::Source, O> decode::Cancel
    for Encoder<'a, 'req, 'd, F, E, Q, O>
{
    type Target = stream::Key;

    fn cancel(self) -> Self::Target {
        self.stream
    }
}
