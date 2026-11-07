use core::{mem, num::NonZero};

use o3::{
    buffer,
    buffer::{Complete as _, hold, resident},
    mem::Rejected,
};
use sark_core::{
    body::{
        self, admit,
        admit::{Length, Retention},
        credit,
    },
    request, route,
};
use sark_http::{
    h1::framing::RequestBody,
    payload::owned,
    request::{Id, Layout},
};

use crate::{
    session,
    wire::{
        Failure, InputError,
        body::{Payload, View, fixed},
        head, parsing,
        parsing::{framing, framing::Parsed},
        response,
    },
};

type Split<'req, 'body, 'd, R, C> = Result<
    (
        Parsed<&'req [u8], R>,
        body::Body<View<'body, 'd>>,
        usize,
        head::Connection,
        C,
        response::Context,
    ),
    parsing::Request<<R as route::Spec>::Request>,
>;

type Assembly<'d, R> = Result<
    Result<
        framing::Assembly<'d, R>,
        (
            Rejected<(admit::Track<admit::Finished>, owned::Segments<'d>), body::Error>,
            request::raw::Parsed<
                request::Parts<'d, <<R as route::Spec>::Request as route::Schema>::Frame>,
                <R as route::Spec>::Request,
            >,
            usize,
            head::Connection,
        ),
    >,
    parsing::Request<<R as route::Spec>::Request>,
>;

pub(crate) struct Frame<'a, 'req, 'body, 'd, R: route::Spec, C = framing::Plain> {
    source: parsing::raw::Bound<&'a parsing::Head<'req>, R>,
    headers: <R::Request as route::Schema>::RawHeaders,
    layout: Layout,
    total: usize,
    pub(crate) connection: head::Connection,
    pub(crate) response: response::Context,
    /// The admitted head length shares the completion tag's alignment space.
    body: Payload<'body, 'd, NonZero<u32>>,
    coding: C,
}

impl<'a, 'req: 'body, 'body, 'd, R: route::Spec, C> Frame<'a, 'req, 'body, 'd, R, C> {
    pub(crate) fn total(&self) -> usize {
        self.total
    }

    pub(crate) fn into_parts(self) -> Split<'req, 'body, 'd, R, C> {
        let len = self.body.len();
        let length = self.body.length();
        let (body, head) = self.body.into_parts();
        let fields = framing::Fields {
            source: self.source,
            headers: self.headers,
            len: head,
            layout: self.layout,
        };
        let (request, frame) = fields.into_frame(self.total)?;
        let view = match (<R::Request as route::Schema>::BODY_RETENTION, body) {
            (Retention::Discarded, _) | (_, Payload::Discarded(..)) => View::from_slice(&[]),
            (Retention::Retained, Payload::Inline(..)) => View::from_slice(frame.body()),
            (Retention::Retained, Payload::Chunked(body, ())) => {
                // SAFETY: drive binds the completed decoder to these exact input
                // bytes. Its frozen ranges cover every retained data fragment,
                // sum to len, and stay within the consumed wire prefix.
                let ranges =
                    unsafe { hold::ranges::Set::from_parts(frame.wire(), body.into_ranges(), len) };
                View::from_ranges(ranges)
            }
            (Retention::Retained, Payload::Streamed(body, ())) => View::from_body(body.into_body()),
        };
        let body = view
            .finish::<R>(len, length)
            .map_err(|error| parsing::Request::Wire(Failure::reject(error)))?;
        Ok((
            request,
            body,
            self.total,
            self.connection,
            self.coding,
            self.response,
        ))
    }

    pub(crate) fn drive<RouteState, Ready, Heads>(
        base: framing::Head<'a, 'req, R, C>,
        ingress: &'body mut session::Ingress<'d, RouteState, Ready, Heads>,
        proof: &mut impl credit::Evidence,
    ) -> Result<framing::Progress<'a, 'req, 'body, 'd, R, C>, parsing::Request<R::Request>> {
        const {
            assert!(
                mem::size_of::<Self>()
                    <= mem::size_of::<(
                        parsing::raw::Bound<&'a parsing::Head<'req>, R>,
                        <R::Request as route::Schema>::RawHeaders,
                        usize,
                        usize,
                        head::Connection,
                        u8,
                        usize,
                        C,
                        response::Context,
                        Layout,
                    )>()
            );
            assert!(mem::size_of::<Option<Self>>() == mem::size_of::<Self>());
        }
        let ctx = base.fields.source.head;
        let head = base.fields.len.get() as usize;
        base.check_body()?;
        credit::Evidence::head(proof);
        let (total, body) = match base.body_framing {
            RequestBody::Empty => (head, Payload::Inline(0, ())),
            RequestBody::Fixed(length) => {
                let Some(total) = head.checked_add(length) else {
                    return Err(parsing::Request::too_large());
                };
                if ctx.input().as_slice().len() < total {
                    let used = match ingress.begin_fixed(
                        ctx.input(),
                        head,
                        total,
                        <R::Request as route::Schema>::BODY_RETENTION,
                    ) {
                        Ok(bytes) => bytes,
                        Err(fixed::Error::Bad) => {
                            return Err(parsing::Request::bad());
                        }
                        Err(fixed::Error::Ingress(error)) => {
                            return Err(parsing::Request::ingress(error));
                        }
                    };
                    return Ok(framing::Progress::Pending(framing::Pending::new(
                        base,
                        used,
                        Length::Known(length),
                    )?));
                }
                (total, Payload::Inline(length, ()))
            }
            RequestBody::Chunked => {
                match ingress.decode(
                    ctx.input(),
                    head,
                    R::MAX_BODY,
                    <R::Request as route::Schema>::BODY_RETENTION,
                ) {
                    Ok(None) => {
                        let consumed = ctx.input().as_slice().len();
                        return Ok(framing::Progress::Pending(framing::Pending::new(
                            base,
                            consumed,
                            Length::Unknown,
                        )?));
                    }
                    Ok(Some(done)) => (done.consumed, done.body),
                    Err(error) => return Err(parsing::Request::chunked(error)),
                }
            }
        };
        let framing::Fields {
            source,
            headers,
            len: head,
            layout,
        } = base.fields;
        Ok(framing::Progress::Complete(Self {
            source,
            headers,
            layout,
            total,
            connection: base.connection,
            response: base.response,
            body: body.with_head(head),
            coding: base.coding,
        }))
    }
}

impl<'a, 'req: 'body, 'body, 'd, R: route::Spec<Request: route::Own>>
    Frame<'a, 'req, 'body, 'd, R>
{
    pub(crate) fn assemble(self, id: Id<'d>) -> Assembly<'d, R> {
        use sark_core::manifold::task::request::Input;

        let Self {
            source,
            headers,
            layout,
            total,
            connection: close,
            response,
            body,
            coding: framing::Plain,
        } = self;
        let (body, head) = body.into_parts();
        let fields = framing::Fields {
            source,
            headers,
            len: head,
            layout,
        };
        let len = body.len();
        let length = body.length();
        let track = admit::Plan::new(
            <R::Request as route::Schema>::BODY_RETENTION,
            R::MAX_BODY,
            length,
            <<R::Request as route::Own>::Mode as route::Mode>::STREAM,
        )
        .try_admit(R::MAX_BODY, len)
        .map_err(|error| parsing::Request::Wire(Failure::Admission(error)))?;
        if matches!(
            body,
            Payload::Inline(..) | Payload::Chunked(..) | Payload::Discarded(..)
        ) && (len == 0 || track.retention() == Retention::Discarded)
            && let Some(request) = <R::Request as route::Own>::empty_task(id)
        {
            fields.into_frame(total)?;
            body::Body::try_new(track, owned::Segments::new()).map_err(|rejected| {
                parsing::Request::Wire(Failure::Bind(request::BindError::Body(*rejected.error())))
            })?;
            return Ok(Ok(framing::Assembly {
                input: Input::Ready { request },
                total,
                connection: close,
                response,
            }));
        }
        let framing::Fields {
            source,
            headers: mut raw,
            len: head,
            layout,
        } = fields;
        source.head.parse_query::<R>(&mut raw)?;
        let retain = || {
            source
                .head
                .input()
                .try_get(0..total)
                .map_err(InputError::Range)?
                .try_retain()
        };
        let (wire, body) = match (body, track.retention()) {
            (Payload::Streamed(body, ()), _) => {
                let wire = source
                    .head
                    .input()
                    .try_head(head)
                    .map_err(parsing::Request::input)?;
                (wire, body.take())
            }
            (_, Retention::Discarded) | (Payload::Discarded(..), _) => {
                let wire = source
                    .head
                    .input()
                    .try_head(head)
                    .map_err(parsing::Request::input)?;
                (wire, owned::Segments::new())
            }
            (Payload::Inline(..), Retention::Retained) => {
                let hold = retain().map_err(parsing::Request::input)?;
                let (wire, tail) = match buffer::raw::Segment::try_split(hold, head.get() as usize)
                {
                    Ok(parts) => parts,
                    Err((error, _)) => {
                        return Err(parsing::Request::Wire(Failure::Bind(
                            request::BindError::Retain(error),
                        )));
                    }
                };
                let body = tail.map_or_else(owned::Segments::new, |tail| {
                    owned::Segments::from(resident::Shared::from(tail))
                });
                (wire, body)
            }
            (Payload::Chunked(body, ()), Retention::Retained) => {
                let hold = retain().map_err(parsing::Request::input)?;
                // SAFETY: this owner retains the exact consumed source prefix
                // admitted by drive. The completed decoder borrow freezes its
                // nonempty ranges, their bounded count, and their exact total.
                let ranges =
                    unsafe { hold::ranges::Set::from_parts(&hold, body.into_ranges(), len) };
                let parts = ranges
                    .try_retain()
                    .map_err(|error| parsing::Request::Wire(Failure::View(error)))?;
                let body = owned::Segments::from(parts);
                let wire = hold
                    .view()
                    .try_get(0..head.get() as usize)
                    .map_err(parsing::Request::range)?
                    .try_retain()
                    .map_err(|error| {
                        parsing::Request::Wire(Failure::Input(InputError::Share(error)))
                    })?;
                (wire, body)
            }
        };
        // SAFETY: every branch retains the admitted offset-zero head prefix.
        // Inline frames prove total >= head before splitting; sharing or
        // copying preserves the layout and both schema field sets' byte origin.
        let request = unsafe { request::raw::Parsed::try_bind(wire, layout, source.params, raw) }
            .map_err(parsing::Request::bind)?;
        let request = request.with_id(id);
        Ok(match body::Body::try_new(track, body) {
            Ok(body) => Ok(framing::Assembly {
                input: Input::Buffered(request.with_body(body)),
                total,
                connection: close,
                response,
            }),
            Err(failure) => Err((failure, request, total, close)),
        })
    }
}
