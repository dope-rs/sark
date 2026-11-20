use fields::Fields;
use sark_core::{manifold::task::request::Input, request, route};
use sark_http::{
    h1::{field::expect, framing::RequestBody, head::line::Request},
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

pub(crate) struct Assembly<'d, R: route::Spec<Request: route::Own>> {
    pub(crate) input: Input<'d, R>,
    pub(crate) total: usize,
    pub(crate) connection: Connection,
    pub(crate) response: response::Context,
}

pub(crate) enum Progress<'a, 'req, 'body, 'd, R: route::Spec> {
    Complete(Frame<'a, 'req, 'body, 'd, R>),
    Pending(Pending<R>),
}

pub(crate) enum Discarded<'a, 'req, R: route::Spec> {
    Complete(Discard<'a, 'req, R>),
    Pending(Pending<R>),
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

pub enum HeadProgress<
    'a,
    'req,
    R: route::Spec,
    A: head::Admission = (),
    const NEGOTIATE: bool = false,
> {
    Complete(Head<'a, 'req, R, A>),
    Pending(PendingHead<R, A, NEGOTIATE>),
}

/// Complete typed request headers tied to their admitted input window.
/// Body admission rejects GET/HEAD with a nonempty fixed body or any chunked body.
/// A fixed length above the route limit is rejected first as too large.
pub struct Head<'a, 'req, R: route::Spec, A: head::Admission = ()> {
    pub(in crate::wire::parsing::framing) fields: Fields<'a, 'req, R>,
    pub(in crate::wire::parsing::framing) connection: head::Connection,
    pub(in crate::wire::parsing::framing) response: response::Context,
    pub(in crate::wire::parsing::framing) body_framing: RequestBody,
    pub(in crate::wire::parsing::framing) expectation: expect::Decision,
    pub(in crate::wire::parsing::framing) allowance: Allowance,
    admission: A::Admitted,
}

impl<'a, 'req, R: route::Spec, A: head::Admission> Head<'a, 'req, R, A> {
    pub const fn source(&self) -> &'a parsing::Head<'req> {
        self.fields.source.head
    }

    pub const fn response(&self) -> response::Context {
        self.response
    }

    /// Removes route-local header admission without changing its frame proof.
    pub fn into_admission(self) -> (Head<'a, 'req, R>, A::Admitted) {
        (
            Head {
                fields: self.fields,
                connection: self.connection,
                response: self.response,
                body_framing: self.body_framing,
                expectation: self.expectation,
                allowance: self.allowance,
                admission: (),
            },
            self.admission,
        )
    }

    pub fn start<const NEGOTIATE: bool>(
        source: parsing::raw::Bound<&'a parsing::Head<'req>, R>,
    ) -> Result<HeadProgress<'a, 'req, R, A, NEGOTIATE>, parsing::Request<R::Request>> {
        let parsed = head::Block::<<R::Request as route::Schema>::RawHeaders, A>::scan::<
            R::Request,
            NEGOTIATE,
        >(
            source.head.input().as_slice(),
            source.head.request().end(),
            MAX_HEADER_COUNT,
            source.head.request().version(),
        )
        .map_err(parsing::Request::Head)?;
        Ok(match parsed {
            head::Scanned::Complete(block) => {
                let parsed = head::Parsed::from_block::<R::Request>(block, source.head.request())
                    .map_err(parsing::Request::Head)?;
                HeadProgress::Complete(Self::new(source, parsed))
            }
            head::Scanned::Pending(partial) => {
                let fields = partial
                    .retain::<R::Request, NEGOTIATE>(*source.head.request(), MAX_HEADER_COUNT)
                    .map_err(parsing::Request::Head)?;
                HeadProgress::Pending(PendingHead {
                    fields,
                    params: source.params,
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
    pub fn parse<const NEGOTIATE: bool>(
        source: parsing::raw::Bound<&'a parsing::Head<'req>, R>,
    ) -> Result<Option<Self>, parsing::Request<R::Request>> {
        let Some(parsed) = head::Parsed::<<R::Request as route::Schema>::RawHeaders, A>::parse::<
            R::Request,
            NEGOTIATE,
        >(source.head, MAX_HEADER_COUNT)
        .map_err(parsing::Request::Head)?
        else {
            return Ok(None);
        };
        Ok(Some(Self::new(source, parsed)))
    }
}

/// Completed route fields bound to the sole append-only owner of a split head.
/// Resumed body admission follows [`Head`]'s method and length checks.
pub struct PendingHead<R: route::Spec, A = (), const NEGOTIATE: bool = false> {
    pub(in crate::wire::parsing::framing) fields:
        head::Pending<<R::Request as route::Schema>::RawHeaders, A, NEGOTIATE>,
    pub(in crate::wire::parsing::framing) params: <R::Request as route::Schema>::RawParams,
}

impl<R: route::Spec, A: head::Admission, const NEGOTIATE: bool> PendingHead<R, A, NEGOTIATE> {
    /// Pending fields retain the head admitted within HEAD_CAPACITY.
    pub fn len(&self) -> u32 {
        self.fields.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn try_start<'d, P, D, H>(
        source: parsing::raw::Bound<parsing::Owned, R>,
        permit: session::Permit,
        pending: impl FnOnce(Self) -> H,
        complete: impl for<'a, 'req> FnOnce(
            Head<'a, 'req, R, A>,
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
        let progress =
            head::Pending::<_, A, NEGOTIATE>::try_start::<R::Request>(owner, MAX_HEADER_COUNT)
                .map_err(parsing::Request::Head)?;
        Ok(Self::settle(progress, params, permit, pending, complete))
    }

    pub fn try_resume<'d, P, D, H>(
        self,
        input: crate::Input<'_>,
        permit: session::Permit,
        pending: impl FnOnce(Self) -> H,
        complete: impl for<'a, 'req> FnOnce(
            Head<'a, 'req, R, A>,
            session::Permit,
        )
            -> session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
    ) -> Result<
        session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
        parsing::Request<R::Request>,
    > {
        let prior = self.len() as usize;
        let Self { fields, params } = self;
        let progress = fields
            .advance::<R::Request>(input.as_slice())
            .map_err(parsing::Request::Head)?;
        Ok(Self::settle(
            progress,
            params,
            permit,
            pending,
            |head, permit| complete(head, permit).after_head(prior),
        ))
    }

    fn settle<'d, P, D, H>(
        progress: head::Progress<<R::Request as route::Schema>::RawHeaders, A, NEGOTIATE>,
        params: <R::Request as route::Schema>::RawParams,
        permit: session::Permit,
        pending: impl FnOnce(Self) -> H,
        complete: impl for<'a, 'req> FnOnce(
            Head<'a, 'req, R, A>,
            session::Permit,
        )
            -> session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>>,
    ) -> session::RouteOutcome<'d, P, D, H, parsing::Rejected<'d>> {
        match progress {
            head::Progress::Pending(fields) => session::RouteOutcome::AwaitHead {
                permit,
                pending: pending(Self { fields, params }),
            },
            head::Progress::Complete(owner, parsed) => owner.with_shared(|view, request| {
                let source = parsing::Head {
                    input: crate::Input::Buffered(view),
                    request,
                };
                let matched = parsing::raw::Bound::retained(&source, params);
                let head = Head::new(matched, parsed);
                complete(head, permit)
            }),
        }
    }
}
