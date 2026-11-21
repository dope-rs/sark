use core::ops::ControlFlow;

use o3::buffer::{self, Complete as _, hold, resident};
use sark_core::{
    body::{
        self, admit,
        admit::{Length, Retention},
        credit,
    },
    manifold, request, route,
    route::Project as _,
};
use sark_http::{
    h1::framing::RequestBody,
    payload::{compress::Compression, owned, response::dispatch},
    request::Id,
};

use crate::{
    route::task::{Mode, Pending},
    session, switch,
    wire::{
        Failure,
        body::{Frame, Payload, View, fixed},
        output, parsing,
        parsing::{framing, framing::Parsed},
        response,
    },
};

impl<'a, 'req, R: route::Spec> framing::Head<'a, 'req, R> {
    fn read<'body, 'd, const EARLY_DISCARD: bool, P, Ready, H>(
        &self,
        ingress: &'body mut session::Ingress<'d, P, Ready, H>,
        proof: &mut impl credit::Evidence,
    ) -> Result<ControlFlow<(usize, Length), Frame<'body, 'd>>, parsing::Request<R::Request>> {
        let input = self.fields.source.head.input();
        let head = self.fields.len.get() as usize;
        self.check_body::<false>()?;
        proof.head();
        let (consumed, body) = match self.body_framing {
            RequestBody::Empty => (head, Payload::Inline(0)),
            RequestBody::Fixed(len)
                if EARLY_DISCARD
                    && <R::Request as route::Schema>::BODY_RETENTION == Retention::Discarded =>
            {
                (head, Payload::Inline(len))
            }
            RequestBody::Fixed(len) => {
                let total = head
                    .checked_add(len)
                    .ok_or_else(parsing::Request::too_large)?;
                if input.as_slice().len() < total {
                    let consumed = ingress
                        .begin_fixed(
                            input,
                            head,
                            total,
                            <R::Request as route::Schema>::BODY_RETENTION,
                        )
                        .map_err(|error| match error {
                            fixed::Error::Bad => parsing::Request::bad(),
                            fixed::Error::Ingress(error) => parsing::Request::ingress(error),
                        })?;
                    return Ok(ControlFlow::Break((consumed, Length::Known(len))));
                }
                (total, Payload::Inline(len))
            }
            RequestBody::Chunked => {
                return Ok(
                    match ingress
                        .decode(
                            input,
                            head,
                            R::MAX_BODY,
                            <R::Request as route::Schema>::BODY_RETENTION,
                        )
                        .map_err(parsing::Request::chunked)?
                    {
                        Some(frame) => ControlFlow::Continue(frame),
                        None => ControlFlow::Break((input.as_slice().len(), Length::Unknown)),
                    },
                );
            }
        };
        Ok(ControlFlow::Continue(Frame { consumed, body }))
    }

    pub(crate) fn request<'body, 'd, P, Ready, H>(
        self,
        ingress: &'body mut session::Ingress<'d, P, Ready, H>,
        proof: &mut impl credit::Evidence,
    ) -> Result<
        framing::Progress<
            (
                Parsed<&'req [u8], R>,
                body::Body<View<'body, 'd>>,
                usize,
                response::Context,
            ),
            R,
        >,
        parsing::Request<R::Request>,
    >
    where
        'req: 'body,
    {
        let frame = match self.read::<false, _, _, _>(ingress, proof)? {
            ControlFlow::Continue(frame) => frame,
            ControlFlow::Break((used, length)) => {
                return Ok(framing::Progress::Pending(framing::Pending::new(
                    self, used, length,
                )?));
            }
        };
        let response = self.response;
        let consumed = frame.consumed;
        let (request, body) = self.bind(frame)?;
        Ok(framing::Progress::Complete((
            request, body, consumed, response,
        )))
    }

    fn bind<'body, 'd>(
        self,
        frame: Frame<'body, 'd>,
    ) -> Result<(Parsed<&'req [u8], R>, body::Body<View<'body, 'd>>), parsing::Request<R::Request>>
    where
        'req: 'body,
    {
        let Frame { consumed, body } = frame;
        let len = body.len();
        let length = body.length();
        let (request, frame) = self.fields.into_frame(consumed)?;
        let view = match (<R::Request as route::Schema>::BODY_RETENTION, body) {
            (Retention::Discarded, _) | (_, Payload::Discarded(_)) => View::Slice(&[]),
            (Retention::Retained, Payload::Inline(_)) => View::Slice(frame.body()),
            (Retention::Retained, Payload::Chunked(body)) => {
                // SAFETY: read completed this decoder on the same input prefix.
                // Its frozen ranges cover exactly len bytes within that prefix.
                let ranges =
                    unsafe { hold::ranges::Set::from_parts(frame.wire(), body.into_ranges(), len) };
                View::Ranges(ranges)
            }
            (Retention::Retained, Payload::Streamed(body)) => View::Owned(body.into_body()),
        };
        let body = view
            .finish::<R>(len, length)
            .map_err(|error| parsing::Request::Wire(Failure::reject(error)))?;
        Ok((request, body))
    }
}

impl<'a, 'req, R: route::Spec<Kind = dispatch::Sync> + crate::route::PolicyOf>
    framing::Head<'a, 'req, R>
{
    pub fn sync<'d, P, Ready, H, S>(
        self,
        ingress: &mut session::Ingress<'d, P, Ready, H>,
        proof: &mut impl credit::Evidence,
        state: &S,
        output: output::Egress<'_>,
        gzip: &mut impl Compression,
    ) -> Result<
        framing::Progress<(session::Consumption, output::Complete<'d>), R>,
        parsing::Request<R::Request>,
    >
    where
        R: manifold::Route<S>,
    {
        let frame = match self.read::<true, _, _, _>(ingress, proof)? {
            ControlFlow::Continue(frame) => frame,
            ControlFlow::Break((used, length)) => {
                return Ok(framing::Progress::Pending(framing::Pending::new(
                    self, used, length,
                )?));
            }
        };
        self.respond(frame, state, output, gzip)
            .map(framing::Progress::Complete)
    }

    pub fn sync_empty<'d, S>(
        self,
        proof: &mut impl credit::Evidence,
        state: &S,
        output: output::Egress<'_>,
        gzip: &mut impl Compression,
    ) -> Result<(session::Consumption, output::Complete<'d>), parsing::Request<R::Request>>
    where
        R: manifold::Route<S>,
    {
        self.check_body::<true>()?;
        proof.head();
        let consumed = self.fields.len.get() as usize;
        self.respond(
            Frame {
                consumed,
                body: Payload::Inline(0),
            },
            state,
            output,
            gzip,
        )
    }

    fn respond<'body, 'd, S>(
        self,
        frame: Frame<'body, 'd>,
        state: &S,
        output: output::Egress<'_>,
        gzip: &mut impl Compression,
    ) -> Result<(session::Consumption, output::Complete<'d>), parsing::Request<R::Request>>
    where
        R: manifold::Route<S>,
        'req: 'body,
    {
        let (head, mut body, used) = match <R::Request as route::Schema>::BODY_RETENTION {
            Retention::Retained => {
                let used = session::Consumption::Buffered(frame.consumed);
                let (head, body) = self.bind(frame)?;
                (head, body, used)
            }
            Retention::Discarded => {
                let length = frame.body.length();
                let used = match length {
                    Length::Known(len) => {
                        session::Consumption::Discard(session::Discarded::new(frame.consumed, len))
                    }
                    Length::Unknown => session::Consumption::Buffered(frame.consumed),
                };
                let head = self.fields.into_head()?;
                let body = View::Slice(&[])
                    .finish::<R>(frame.body.len(), length)
                    .map_err(|error| parsing::Request::Wire(Failure::reject(error)))?;
                (head, body, used)
            }
        };
        let reply = <<R::Request as route::Schema>::Body as body::Project>::try_project(
            &mut body,
            (),
            |view| {
                let request =
                    R::Request::try_view(&head, view).map_err(Failure::route::<R::Request>)?;
                output.try_route(R::invoke(request, state), gzip, R::POLICY)
            },
        )
        .map_err(|error| parsing::Request::Wire(Failure::Projection(error.discard())))?
        .map_err(parsing::Request::Wire)?;
        Ok((used, reply))
    }
}

impl<'d, Runtime, Stored, Transitions: switch::Engine, Ready, Heads>
    session::Conn<'d, Runtime, Stored, Transitions, Ready, Heads>
{
    pub fn dispatch_task<R, Admit, Feed, E>(
        &mut self,
        id: Id<'d>,
        found: framing::Head<'_, '_, R>,
        admit: Admit,
        feed: Feed,
        mut proof: E,
    ) -> session::RouteOutcome<'d, Pending<'d, R>, Transitions::Raw>
    where
        R: route::Spec<Request: route::Own>,
        <R::Request as route::Own>::Mode: Mode<'d, R>,
        Admit: FnOnce(
            <R::Request as route::Own>::Input<'d>,
            usize,
            response::Context,
            &mut Runtime,
        ) -> session::Outcome<'d, Transitions::Raw>,
        Feed: FnOnce(
            Pending<'d, R>,
            owned::Segments<'d>,
            E,
        ) -> Result<Pending<'d, R>, sark_http::payload::fixed::Body<'d>>,
        E: credit::Evidence,
    {
        let failure = 'request: {
            let Frame {
                consumed: total,
                body,
            } = match found.read::<false, _, _, _>(&mut self.ingress, &mut proof) {
                Ok(ControlFlow::Continue(frame)) => frame,
                Ok(ControlFlow::Break((used, length))) => {
                    let wait = match framing::Pending::new(found, used, length) {
                        Ok(wait) => wait,
                        Err(error) => return session::Outcome::from(error.into_rejection()).into(),
                    };
                    let req = wait.head.with_id(id);
                    let plan = admit::Plan::new(
                        <R::Request as route::Schema>::BODY_RETENTION,
                        R::MAX_BODY,
                        wait.declared_body_len,
                        <<R::Request as route::Own>::Mode as route::Mode>::STREAM,
                    );
                    let open = match plan.try_open(R::MAX_BODY) {
                        Ok(open) => open,
                        Err(error) => break 'request Failure::Admission(error),
                    };
                    let body = match <<R::Request as route::Own>::Mode as Mode<'d, R>>::begin(
                        req,
                        open,
                        wait.response,
                    ) {
                        Ok(body) => body,
                        Err(error) => {
                            let reply =
                                Failure::Route(<R::Request as route::Own>::reject(error)).body();
                            return session::Outcome::Close(reply).into();
                        }
                    };
                    let parts = self.ingress.payload.take();
                    let body = match feed(body, parts, proof) {
                        Ok(body) => body,
                        Err(reply) => return session::Outcome::Close(reply).into(),
                    };
                    return session::RouteOutcome::AwaitBody {
                        state: wait.state,
                        expectation: wait.expectation,
                        pending: body,
                    };
                }
                Err(reason) => {
                    return session::Outcome::from(reason.into_rejection()).into();
                }
            };
            let response = found.response;
            let framing::Fields {
                source,
                headers: mut raw,
                len: head,
                layout,
            } = found.fields;
            let len = body.len();
            let length = body.length();
            let plan = admit::Plan::new(
                <R::Request as route::Schema>::BODY_RETENTION,
                R::MAX_BODY,
                length,
                <<R::Request as route::Own>::Mode as route::Mode>::STREAM,
            );
            let track = match plan.try_admit(R::MAX_BODY, len) {
                Ok(track) => track,
                Err(error) => break 'request Failure::Admission(error),
            };
            let input = if matches!(
                body,
                Payload::Inline(..) | Payload::Chunked(..) | Payload::Discarded(..)
            ) && (len == 0 || track.retention() == Retention::Discarded)
                && let Some(request) = <R::Request as route::Own>::empty_task(id)
            {
                let fields = framing::Fields {
                    source,
                    headers: raw,
                    len: head,
                    layout,
                };
                if let Err(error) = fields.into_frame(total) {
                    return session::Outcome::from(error.into_rejection()).into();
                }
                request.into()
            } else {
                if let Err(error) = source.head.parse_query::<R>(&mut raw) {
                    return session::Outcome::from(error.into_rejection()).into();
                }
                let input = source.head.input();
                let (wire, body) = match (body, track.retention()) {
                    (Payload::Inline(..), Retention::Retained) => {
                        let hold = match input.try_retain_prefix(total) {
                            Ok(hold) => hold,
                            Err(error) => break 'request Failure::Input(error),
                        };
                        let (wire, tail) =
                            match buffer::raw::Segment::try_split(hold, head.get() as usize) {
                                Ok(parts) => parts,
                                Err((error, _)) => {
                                    break 'request Failure::Bind(request::BindError::Retain(
                                        error,
                                    ));
                                }
                            };
                        let body = tail.map_or_else(owned::Segments::new, |tail| {
                            owned::Segments::from(resident::Shared::from(tail))
                        });
                        (wire, body)
                    }
                    (Payload::Chunked(body), Retention::Retained) => {
                        let hold = match input.try_retain_prefix(total) {
                            Ok(hold) => hold,
                            Err(error) => break 'request Failure::Input(error),
                        };
                        // SAFETY: this owner retains the exact consumed source prefix
                        // admitted by drive. The completed decoder borrow freezes its
                        // nonempty ranges, their bounded count, and their exact total.
                        let ranges = unsafe {
                            hold::ranges::Set::from_parts(&hold, body.into_ranges(), len)
                        };
                        let parts = match ranges.try_retain() {
                            Ok(parts) => parts,
                            Err(error) => break 'request Failure::View(error),
                        };
                        let body = owned::Segments::from(parts);
                        let wire = match crate::Input::Buffered(hold.view())
                            .try_retain_prefix(head.get() as usize)
                        {
                            Ok(wire) => wire,
                            Err(error) => break 'request Failure::Input(error),
                        };
                        (wire, body)
                    }
                    (payload, _) => {
                        let wire = match input.try_head(head) {
                            Ok(wire) => wire,
                            Err(error) => break 'request Failure::Input(error),
                        };
                        let body = match payload {
                            Payload::Streamed(body) => body.take(),
                            Payload::Inline(..) | Payload::Chunked(..) | Payload::Discarded(..) => {
                                owned::Segments::new()
                            }
                        };
                        (wire, body)
                    }
                };
                // SAFETY: every branch retains the admitted offset-zero head prefix.
                // Inline frames prove total >= head before splitting; sharing or
                // copying preserves the layout and both schema field sets' byte origin.
                let request = match unsafe {
                    request::raw::Parsed::try_bind(wire, layout, source.params, raw)
                } {
                    Ok(request) => request,
                    Err(error) => break 'request Failure::Bind(error),
                };
                let request = request.with_id(id);
                let body = match body::Body::try_new(track, body) {
                    Ok(body) => body,
                    Err(rejected) => {
                        break 'request Failure::Bind(request::BindError::Body(rejected.discard()));
                    }
                };
                request.with_body(body).into()
            };
            return admit(input, total, response, &mut self.async_state).into();
        };
        session::Outcome::Failed(failure).into()
    }
}
