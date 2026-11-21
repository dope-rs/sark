use core::ops::ControlFlow;

use fields::Fields;
use sark_core::{request, route};
use sark_http::{
    h1::{field::expect, framing::RequestBody},
    method::Known,
};

use crate::wire::{head, parsing, response};

pub(crate) type Parsed<B, R> = request::raw::Parsed<
    request::Head<B, <<R as route::Spec>::Request as route::Schema>::Frame>,
    <R as route::Spec>::Request,
>;

mod fields;
mod frame;
mod pending;

pub use pending::Pending;

pub enum Progress<T, R: route::Spec> {
    Complete(T),
    Pending(Pending<R>),
}

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
    pub(in crate::wire::parsing::framing) response: response::Context,
    pub(in crate::wire::parsing::framing) body_framing: RequestBody,
    pub(in crate::wire::parsing::framing) expectation: expect::Decision,
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
                response: self.response,
                body_framing: self.body_framing,
                expectation: self.expectation,
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

    pub(in crate::wire::parsing::framing) fn new(
        source: parsing::raw::Bound<&'a parsing::Head<'req>, R>,
        parsed: head::Parsed<<R::Request as route::Schema>::RawHeaders, A>,
    ) -> Self {
        let response = parsed.response();
        let body_framing = parsed.body();
        let expectation = parsed.expectation();
        let len = parsed.len();
        let layout = parsed.layout();
        let (headers, admission) = parsed.into_fields();
        Self {
            response,
            body_framing,
            expectation,
            fields: Fields {
                len,
                layout,
                headers,
                source,
            },
            admission,
        }
    }

    fn check_body<const EMPTY: bool>(&self) -> Result<(), parsing::Request<R::Request>> {
        match self.body_framing {
            RequestBody::Fixed(len) if len > R::MAX_BODY => Err(parsing::Request::too_large()),
            RequestBody::Empty | RequestBody::Fixed(0) => Ok(()),
            _ if EMPTY => Err(parsing::Request::bad()),
            _ => {
                let line = self.fields.source.head.request();
                match line.matched() {
                    Some(Known::Get | Known::Head) => Err(parsing::Request::bad()),
                    None if matches!(line.method(), b"GET" | b"HEAD") => {
                        Err(parsing::Request::bad())
                    }
                    _ => Ok(()),
                }
            }
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

    pub fn try_start<T>(
        source: parsing::raw::Bound<parsing::Owned, R>,
        complete: impl FnOnce(Head<'_, '_, R, A>) -> T,
    ) -> Result<ControlFlow<Self, T>, parsing::Request<R::Request>> {
        let parsing::raw::Bound { head, params } = source;
        let progress =
            head::Pending::<_, A, NEGOTIATE>::try_start::<R::Request>(head, MAX_HEADER_COUNT)
                .map_err(parsing::Request::Head)?;
        Ok(Self::settle(progress, params, complete))
    }

    pub fn try_resume<T>(
        self,
        input: crate::Input<'_>,
        complete: impl FnOnce(Head<'_, '_, R, A>) -> T,
    ) -> Result<ControlFlow<Self, T>, parsing::Request<R::Request>> {
        let Self { fields, params } = self;
        let progress = fields
            .advance::<R::Request>(input.as_slice())
            .map_err(parsing::Request::Head)?;
        Ok(Self::settle(progress, params, complete))
    }

    fn settle<T>(
        progress: head::Progress<<R::Request as route::Schema>::RawHeaders, A, NEGOTIATE>,
        params: <R::Request as route::Schema>::RawParams,
        complete: impl FnOnce(Head<'_, '_, R, A>) -> T,
    ) -> ControlFlow<Self, T> {
        match progress {
            head::Progress::Pending(fields) => ControlFlow::Break(Self { fields, params }),
            head::Progress::Complete(owner, parsed) => owner.with_shared(|view, request| {
                let head = parsing::Head {
                    input: crate::Input::Buffered(view),
                    request,
                };
                ControlFlow::Continue(complete(Head::new(
                    parsing::raw::Bound::retained(&head, params),
                    parsed,
                )))
            }),
        }
    }
}
