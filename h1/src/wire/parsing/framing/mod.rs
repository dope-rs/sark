use core::{convert, marker::PhantomData};

use fields::Fields;
use sark_core::{manifold::task::request::Input, request, route};
use sark_http::{
    h1::{
        field::expect,
        framing::RequestBody,
        head::{line::Request, request::Coding},
    },
    method::Known,
};

use crate::{
    session,
    wire::{head, head::Connection, parsing, response},
};

pub(crate) type Parsed<B, R> = request::raw::Parsed<
    request::Head<B, <<R as route::Spec>::Request as route::Schema>::Frame>,
    <R as route::Spec>::Request,
>;

mod discard;
mod fields;
mod frame;
mod pending;

pub(crate) use discard::Discard;
pub(crate) use frame::Frame;
pub(crate) use pending::Pending;

pub struct Plain;

pub(crate) struct Assembly<'d, R: route::Spec<Request: route::Own>> {
    pub(crate) input: Input<'d, R>,
    pub(crate) total: usize,
    pub(crate) connection: Connection,
    pub(crate) response: response::Context,
}

pub(crate) enum Progress<'a, 'req, 'body, 'd, R: route::Spec, C = Plain> {
    Complete(Frame<'a, 'req, 'body, 'd, R, C>),
    Pending(Pending<R, C>),
}

pub(crate) enum Discarded<'a, 'req, R: route::Spec> {
    Complete(Discard<'a, 'req, R>),
    Pending(Pending<R, Coding>),
}

enum Allowance {
    Allowed,
    Forbidden,
}

impl Allowance {
    fn from(request: &Request<'_>) -> Self {
        match request.matched() {
            Some(Known::Get | Known::Head) => Self::Forbidden,
            Some(_) => Self::Allowed,
            None => {
                if request.method() == b"GET" || request.method() == b"HEAD" {
                    Self::Forbidden
                } else {
                    Self::Allowed
                }
            }
        }
    }
}

const _: () = assert!(core::mem::size_of::<Allowance>() == 1);

const MAX_HEADER_COUNT: usize = 128;

pub enum HeadProgress<'a, 'req, R: route::Spec, C = Plain, A: head::Admission = ()> {
    Complete(Head<'a, 'req, R, C, A>),
    Pending(PendingHead<R, C, A>),
}

/// Complete typed request headers tied to their admitted input window.
/// Body admission rejects GET/HEAD with a nonempty fixed body or any chunked body.
/// A fixed length above the route limit is rejected first as too large.
pub struct Head<'a, 'req, R: route::Spec, C = Plain, A: head::Admission = ()> {
    pub(in crate::wire::parsing::framing) fields: Fields<'a, 'req, R>,
    pub(in crate::wire::parsing::framing) connection: head::Connection,
    pub(in crate::wire::parsing::framing) response: response::Context,
    pub(in crate::wire::parsing::framing) coding: C,
    pub(in crate::wire::parsing::framing) body_framing: RequestBody,
    pub(in crate::wire::parsing::framing) expectation: expect::Decision,
    pub(in crate::wire::parsing::framing) allowance: Allowance,
    admission: A::Admitted,
}

impl<'a, 'req, R: route::Spec, C, A: head::Admission> Head<'a, 'req, R, C, A> {
    pub const fn source(&self) -> &'a parsing::Head<'req> {
        self.fields.source.head
    }

    pub const fn response(&self) -> response::Context {
        self.response
    }

    /// Removes route-local header admission without changing its frame proof.
    pub fn into_admission(self) -> (Head<'a, 'req, R, C>, A::Admitted) {
        (
            Head {
                fields: self.fields,
                connection: self.connection,
                response: self.response,
                coding: self.coding,
                body_framing: self.body_framing,
                expectation: self.expectation,
                allowance: self.allowance,
                admission: (),
            },
            self.admission,
        )
    }

    fn start_with(
        source: parsing::raw::Bound<&'a parsing::Head<'req>, R>,
        coding: Coding,
        select: impl FnOnce(Coding) -> C,
    ) -> Result<HeadProgress<'a, 'req, R, C, A>, parsing::Request<R::Request>> {
        let parsed =
            head::Block::<<R::Request as route::Schema>::RawHeaders, A>::scan::<R::Request>(
                source.head.input().as_slice(),
                source.head.request().end(),
                MAX_HEADER_COUNT,
                coding,
                source.head.request().version(),
            )
            .map_err(parsing::Request::Head)?;
        Ok(match parsed {
            head::Scanned::Complete(block) => {
                let coding = select(block.coding());
                let parsed = head::Parsed::from_block::<R::Request>(block, source.head.request())
                    .map_err(parsing::Request::Head)?;
                HeadProgress::Complete(Self::new(source, parsed, coding))
            }
            head::Scanned::Pending(partial) => {
                let fields = partial
                    .retain::<R::Request>(*source.head.request(), coding, MAX_HEADER_COUNT)
                    .map_err(parsing::Request::Head)?;
                HeadProgress::Pending(PendingHead {
                    fields,
                    params: source.params,
                    coding: PhantomData,
                })
            }
        })
    }

    pub(crate) const fn connection(&self) -> head::Connection {
        self.connection
    }

    pub(in crate::wire::parsing::framing) fn new(
        source: parsing::raw::Bound<&'a parsing::Head<'req>, R>,
        parsed: head::Parsed<<R::Request as route::Schema>::RawHeaders, A>,
        coding: C,
    ) -> Self {
        let connection = parsed.connection();
        let response = parsed.response();
        let body_framing = parsed.body();
        let expectation = parsed.expectation();
        let len = parsed.len();
        let layout = parsed.layout();
        let (headers, admission) = parsed.into_fields();
        Self {
            connection,
            response,
            coding,
            body_framing,
            expectation,
            allowance: Allowance::from(source.head.request()),
            fields: Fields {
                len,
                layout,
                headers,
                source,
            },
            admission,
        }
    }

    fn check_body(&self) -> Result<(), parsing::Request<R::Request>> {
        match (self.body_framing, &self.allowance) {
            (RequestBody::Fixed(length), _) if length > R::MAX_BODY => {
                Err(parsing::Request::too_large())
            }
            (RequestBody::Empty | RequestBody::Fixed(0), _) | (_, Allowance::Allowed) => Ok(()),
            (_, Allowance::Forbidden) => Err(parsing::Request::bad()),
        }
    }
}

impl<'a, 'req, R: route::Spec, A: head::Admission> Head<'a, 'req, R, Plain, A> {
    pub fn start(
        source: parsing::raw::Bound<&'a parsing::Head<'req>, R>,
    ) -> Result<HeadProgress<'a, 'req, R, Plain, A>, parsing::Request<R::Request>> {
        Self::start_with(source, Coding::Plain, |_| Plain)
    }

    pub fn parse(
        source: parsing::raw::Bound<&'a parsing::Head<'req>, R>,
    ) -> Result<Option<Self>, parsing::Request<R::Request>> {
        let Some(parsed) = head::Parsed::<<R::Request as route::Schema>::RawHeaders, A>::parse::<
            R::Request,
        >(source.head, MAX_HEADER_COUNT)
        .map_err(parsing::Request::Head)?
        else {
            return Ok(None);
        };
        Ok(Some(Self::new(source, parsed, Plain)))
    }
}

impl<'a, 'req, R: route::Spec, A: head::Admission> Head<'a, 'req, R, Coding, A> {
    pub fn start(
        source: parsing::raw::Bound<&'a parsing::Head<'req>, R>,
        coding: Coding,
    ) -> Result<HeadProgress<'a, 'req, R, Coding, A>, parsing::Request<R::Request>> {
        Self::start_with(source, coding, convert::identity)
    }

    pub fn parse(
        source: parsing::raw::Bound<&'a parsing::Head<'req>, R>,
        coding: Coding,
    ) -> Result<Option<Self>, parsing::Request<R::Request>> {
        let Some(parsed) = head::Gzip::<<R::Request as route::Schema>::RawHeaders, A>::parse::<
            R::Request,
        >(source.head, MAX_HEADER_COUNT, coding)
        .map_err(parsing::Request::Head)?
        else {
            return Ok(None);
        };
        let (parsed, coding) = parsed.into_parts();
        Ok(Some(Self::new(source, parsed, coding)))
    }
}

/// Completed route fields bound to the sole append-only owner of a split head.
/// Resumed body admission follows [`Head`]'s method and length checks.
pub struct PendingHead<R: route::Spec, C = Plain, A = ()> {
    pub(in crate::wire::parsing::framing) fields:
        head::Pending<<R::Request as route::Schema>::RawHeaders, A>,
    pub(in crate::wire::parsing::framing) params: <R::Request as route::Schema>::RawParams,
    pub(in crate::wire::parsing::framing) coding: PhantomData<C>,
}

impl<R: route::Spec, C, A: head::Admission> PendingHead<R, C, A> {
    /// Pending fields retain the head admitted within HEAD_CAPACITY.
    pub fn len(&self) -> u32 {
        self.fields.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn try_with<'d, P, D, H>(
        source: parsing::raw::Bound<parsing::Owned, R>,
        coding: Coding,
        permit: session::Permit,
        select: impl FnOnce(Coding) -> C,
        pending: impl FnOnce(Self) -> H,
        complete: impl for<'a, 'req> FnOnce(
            Head<'a, 'req, R, C, A>,
            session::Permit,
        )
            -> session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
    ) -> Result<
        session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
        parsing::Request<R::Request>,
    > {
        let parsing::raw::Bound {
            head: owner,
            params,
        } = source;
        let progress = head::Pending::try_start::<R::Request>(owner, coding, MAX_HEADER_COUNT)
            .map_err(parsing::Request::Head)?;
        Ok(Self::settle(
            progress, params, permit, select, pending, complete,
        ))
    }

    fn advance<'d, P, D, H>(
        self,
        input: crate::Input<'_>,
        permit: session::Permit,
        select: impl FnOnce(Coding) -> C,
        pending: impl FnOnce(Self) -> H,
        complete: impl for<'a, 'req> FnOnce(
            Head<'a, 'req, R, C, A>,
            session::Permit,
        )
            -> session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
    ) -> Result<
        session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
        parsing::Request<R::Request>,
    > {
        let prior = self.len() as usize;
        let Self {
            fields,
            params,
            coding: _,
        } = self;
        let progress = fields
            .advance::<R::Request>(input.as_slice())
            .map_err(parsing::Request::Head)?;
        Ok(Self::settle(
            progress,
            params,
            permit,
            select,
            pending,
            |head, permit| complete(head, permit).after_head(prior),
        ))
    }

    fn settle<'d, P, D, H>(
        progress: head::Progress<<R::Request as route::Schema>::RawHeaders, A>,
        params: <R::Request as route::Schema>::RawParams,
        permit: session::Permit,
        select: impl FnOnce(Coding) -> C,
        pending: impl FnOnce(Self) -> H,
        complete: impl for<'a, 'req> FnOnce(
            Head<'a, 'req, R, C, A>,
            session::Permit,
        )
            -> session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
    ) -> session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>> {
        match progress {
            head::Progress::Pending(fields) => session::RouteOutcome::AwaitHead {
                permit,
                pending: pending(Self {
                    fields,
                    params,
                    coding: PhantomData,
                }),
            },
            head::Progress::Complete(owner, parsed) => owner.with_shared(|view, request| {
                let source = parsing::Head {
                    input: crate::Input::Buffered(view),
                    request,
                };
                let (parsed, coding) = parsed.into_parts();
                let matched = parsing::raw::Bound::retained(&source, params);
                let head = Head::new(matched, parsed, select(coding));
                complete(head, permit)
            }),
        }
    }
}

impl<R: route::Spec, A: head::Admission> PendingHead<R, Plain, A> {
    pub fn try_start<'d, P, D, H>(
        source: parsing::raw::Bound<parsing::Owned, R>,
        permit: session::Permit,
        pending: impl FnOnce(Self) -> H,
        complete: impl for<'a, 'req> FnOnce(
            Head<'a, 'req, R, Plain, A>,
            session::Permit,
        )
            -> session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
    ) -> Result<
        session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
        parsing::Request<R::Request>,
    > {
        Self::try_with(source, Coding::Plain, permit, |_| Plain, pending, complete)
    }

    pub fn try_resume<'d, P, D, H>(
        self,
        input: crate::Input<'_>,
        permit: session::Permit,
        pending: impl FnOnce(Self) -> H,
        complete: impl for<'a, 'req> FnOnce(
            Head<'a, 'req, R, Plain, A>,
            session::Permit,
        )
            -> session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
    ) -> Result<
        session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
        parsing::Request<R::Request>,
    > {
        self.advance(input, permit, |_| Plain, pending, complete)
    }
}

impl<R: route::Spec, A: head::Admission> PendingHead<R, Coding, A> {
    pub fn try_start<'d, P, D, H>(
        source: parsing::raw::Bound<parsing::Owned, R>,
        coding: Coding,
        permit: session::Permit,
        pending: impl FnOnce(Self) -> H,
        complete: impl for<'a, 'req> FnOnce(
            Head<'a, 'req, R, Coding, A>,
            session::Permit,
        )
            -> session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
    ) -> Result<
        session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
        parsing::Request<R::Request>,
    > {
        Self::try_with(source, coding, permit, convert::identity, pending, complete)
    }

    pub fn try_resume<'d, P, D, H>(
        self,
        input: crate::Input<'_>,
        permit: session::Permit,
        pending: impl FnOnce(Self) -> H,
        complete: impl for<'a, 'req> FnOnce(
            Head<'a, 'req, R, Coding, A>,
            session::Permit,
        )
            -> session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
    ) -> Result<
        session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
        parsing::Request<R::Request>,
    > {
        self.advance(input, permit, convert::identity, pending, complete)
    }
}
