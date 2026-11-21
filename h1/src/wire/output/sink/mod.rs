use std::{convert::Infallible, ops::ControlFlow};

use o3::buffer::write::Slice;
use o3::buffer::{bytes::scope, storage};
use sark_http::{
    field,
    h1::head::{
        line::Version,
        request::Coding,
        response::{Date, Server},
    },
    owner::{evidence, handoff, handoff::stage},
    payload::{
        compress::{self, Compression},
        fixed::Body,
        response::{self, Content, Encode, dispatch, encoded, headers, status::Code},
    },
};

use crate::wire::{
    Failure,
    output::Complete,
    response::{Context, framing},
};

pub(super) mod error;
mod gzip;
mod head;

use error::Error;
use gzip::{compress_body, compress_bounded, compress_exact};
use head::{Pending, emit_framed_head, emit_head};

pub(super) enum Mode<'a, C: Compression> {
    Plain,
    Compress { gzip: &'a mut C, required: bool },
}

impl<'a, C: Compression> Mode<'a, C> {
    pub(super) fn new(mode: crate::route::Mode, coding: Coding, gzip: &'a mut C) -> Self {
        match mode {
            crate::route::Mode::Plain => Self::Plain,
            crate::route::Mode::Gzip | crate::route::Mode::RequiredGzip if !coding.gzip() => {
                Self::Plain
            }
            crate::route::Mode::Gzip => Self::Compress {
                gzip,
                required: !coding.identity(),
            },
            crate::route::Mode::RequiredGzip => Self::Compress {
                gzip,
                required: true,
            },
        }
    }
}

fn identity(response: Context, encoded: bool) -> Result<(), Error> {
    if encoded || response.coding().identity() {
        Ok(())
    } else {
        Err(Error::NotAcceptable(response.coding()))
    }
}

#[derive(Clone, Copy)]
pub(super) struct Headers<'a> {
    value: &'a [u8; 29],
    dated: Date,
    server: Server,
    response: Context,
}

pub(super) struct Sink<
    'a,
    'req,
    'd,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
    C: Compression,
    A = Infallible,
> {
    out: Slice<'a, &'a mut usize>,
    date: Headers<'a>,
    mode: Mode<'a, C>,
    slot: handoff::Slot<'req, 'd, Q, O>,
    admit: Option<A>,
}

impl<'a, 'req, 'd, Q: scope::Source, O: stage::Body<'req, 'd>, C: Compression, A>
    Sink<'a, 'req, 'd, Q, O, C, A>
{
    pub(super) const fn new(
        out: Slice<'a, &'a mut usize>,
        date: &'a [u8; 29],
        response: Context,
        policy: crate::route::Policy,
        mode: Mode<'a, C>,
        slot: handoff::Slot<'req, 'd, Q, O>,
        admit: Option<A>,
    ) -> Self {
        Self {
            out,
            date: Headers {
                value: date,
                dated: policy.date,
                server: policy.server,
                response,
            },
            mode,
            slot,
            admit,
        }
    }

    fn omission(&self, status: Code) -> Result<Option<framing::Empty>, Error> {
        if status.is_info() {
            return Err(Error::Informational(status));
        }
        Ok(
            match Content::new(self.date.response.method(), status.as_u16()) {
                Content::Forbidden | Content::Suppressed => Some(framing::Empty::Unframed),
                Content::Empty => Some(framing::Empty::Zero),
                Content::Tunnel => return Err(Error::Tunnel(status)),
                Content::Allowed => None,
            },
        )
    }

    fn empty<H: field::Source<headers::Application>>(
        self,
        status: Code,
        fields: &H,
        frame: framing::Empty,
    ) -> Result<ControlFlow<O::Deferred, Complete<'d>>, Failure> {
        emit_framed_head::<_, _, ()>(self.out, self.date, status, fields, frame)
            .map_err(Error::Wire)?;
        Ok(ControlFlow::Continue(Complete::Inline))
    }
}

impl<'input, 'req, 'd: 'req, Q: scope::Source, O: stage::Body<'req, 'd>, C: Compression, A>
    dispatch::Accept<'d> for Sink<'_, 'req, 'd, Q, O, C, A>
where
    Q::Selection: Into<crate::Input<'input>>,
{
    type Output = ControlFlow<O::Deferred, Complete<'d>>;
    type Error = Failure;

    fn fixed<H, B>(self, status: Code, fields: H, body: B) -> Result<Self::Output, Self::Error>
    where
        H: field::Source<headers::Application>,
        B: response::Body<'d>,
    {
        if let Some(frame) = self.omission(status)? {
            return self.empty(status, &fields, frame);
        }
        let Self {
            out,
            date,
            mode,
            slot,
            ..
        } = self;
        match mode {
            Mode::Compress { gzip, required } => {
                compress_body(out, date, status, &fields, body, gzip, required, slot)
            }
            Mode::Plain => plain_body(out, date, status, &fields, body, slot),
        }
    }

    fn statics<'body, H>(
        self,
        status: Code,
        fields: H,
        body: Body<'body>,
    ) -> Result<Self::Output, Self::Error>
    where
        H: field::Source<headers::Application>,
        'body: 'd,
    {
        if let Some(frame) = self.omission(status)? {
            return self.empty(status, &fields, frame);
        }
        if let Mode::Compress {
            gzip,
            required: true,
        } = self.mode
        {
            return compress_body(
                self.out, self.date, status, &fields, body, gzip, true, self.slot,
            );
        }
        let (_, encoded) = emit_head::<_, bool>(self.out, self.date, status, &fields, body.len())
            .map_err(Error::Wire)?;
        identity(self.date.response, encoded)?;
        Ok(ControlFlow::Continue(if body.is_empty() {
            Complete::Inline
        } else {
            Complete::Static { body }
        }))
    }

    fn encoded<const EXACT: bool>(
        self,
        status: Code,
        fields: impl field::Source<headers::Application>,
        body: impl encoded::Source<EXACT>,
    ) -> Result<Self::Output, Self::Error> {
        if let Some(frame) = self.omission(status)? {
            return self.empty(status, &fields, frame);
        }
        let plan = body.try_plan().map_err(|error| Error::Size(error.into()))?;
        let out = self.out;
        let date = self.date;
        let ready = match self.mode {
            Mode::Compress { gzip, required } if EXACT => {
                compress_exact(out, date, status, &fields, plan, gzip, required)?
            }
            Mode::Compress { gzip, required } => {
                compress_bounded(out, date, status, &fields, plan, gzip, required)?
            }
            Mode::Plain if EXACT => finish_exact(out, date, status, &fields, plan)?,
            Mode::Plain => finish_bounded(out, date, status, &fields, plan)?,
        };
        Ok(ControlFlow::Continue(ready))
    }
}

impl<'input, 'req, 'd: 'req, Q: scope::Source, O: stage::Body<'req, 'd>, C: Compression, A>
    dispatch::Sink<'d> for Sink<'_, 'req, 'd, Q, O, C, A>
where
    Q::Selection: Into<crate::Input<'input>>,
{
    type Stream<S> = Option<(A, S)>;

    fn stream<H, S>(
        self,
        status: Code,
        fields: H,
        stream: Option<S>,
    ) -> Result<Self::Stream<S>, Self::Error>
    where
        H: field::Source<headers::Application>,
    {
        let frame = self.omission(status)?;
        let frame = match self.admit {
            None => framing::Empty::Unframed,
            Some(admit) => match frame {
                Some(frame) => frame,
                None => match stream {
                    None => framing::Empty::Zero,
                    Some(stream) => {
                        if self.date.response.version() == Version::Ten {
                            return Err(Error::StreamingVersion(Version::Ten).into());
                        }
                        let (_, encoded) = emit_framed_head::<_, _, bool>(
                            self.out,
                            self.date,
                            status,
                            &fields,
                            framing::Chunked,
                        )
                        .map_err(Error::Wire)?;
                        if matches!(self.mode, Mode::Compress { required: true, .. }) {
                            return Err(Error::Compression(compress::Error::Unavailable).into());
                        }
                        identity(self.date.response, encoded)?;
                        return Ok(Some((admit, stream)));
                    }
                },
            },
        };
        emit_framed_head::<_, _, ()>(self.out, self.date, status, &fields, frame)
            .map_err(Error::Wire)?;
        Ok(None)
    }
}

fn plain_body<'input, 'req, 'd, H, B, Q: scope::Source, O: stage::Body<'req, 'd>>(
    mut out: Slice<'_, &mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    body: B,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<ControlFlow<O::Deferred, Complete<'d>>, Failure>
where
    H: field::Source<headers::Application>,
    B: response::Body<'d>,
    Q::Selection: Into<crate::Input<'input>>,
{
    let (head, encoded) = emit_head::<_, bool>(out.reborrow(), date, status, fields, body.len())
        .map_err(Error::Wire)?;
    let inline = if B::INLINE { body.as_slice() } else { None };
    if let Some(raw) = inline {
        head.checked_add(raw.len()).ok_or(Error::Overflow {
            base: head,
            added: raw.len(),
        })?;
        if o3::buffer::write::Sink::write_slice(&mut out, raw).is_ok() {
            identity(date.response, encoded)?;
            return Ok(ControlFlow::Continue(Complete::copied(raw.len())));
        }
    }
    let plan = body.try_resolve(&slot).map_err(Error::Owner)?;
    identity(date.response, encoded)?;
    finish(plan)
}

fn finish_exact<'d, H: field::Source<headers::Application>, B: Encode, const EXACT: bool>(
    mut out: Slice<'_, &mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    plan: encoded::Plan<B, EXACT>,
) -> Result<Complete<'d>, Error> {
    let len = plan.limit();
    let (head, encoded) = emit_head::<_, bool>(out.reborrow(), date, status, fields, len)?;
    let written = place_exact(out, head, plan)?;
    identity(date.response, encoded)?;
    Ok(written)
}

fn place_exact<'d, B: Encode, const EXACT: bool>(
    mut out: Slice<'_, &mut usize>,
    head: usize,
    plan: encoded::Plan<B, EXACT>,
) -> Result<Complete<'d>, Error> {
    let len = plan.limit();
    head.checked_add(len).ok_or(Error::Overflow {
        base: head,
        added: len,
    })?;
    if len <= out.remaining() {
        plan.write(&mut out)?;
        return Ok(Complete::copied(len));
    }
    Ok(Complete::shared(plan.into_shared()?))
}

fn finish_bounded<
    'out,
    'd,
    H: field::Source<headers::Application>,
    B: Encode,
    const EXACT: bool,
>(
    mut out: Slice<'out, &'out mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    plan: encoded::Plan<B, EXACT>,
) -> Result<Complete<'d>, Error> {
    let max = plan.limit();
    if let Some(mut pending) = Pending::try_place(&mut out, date, status, fields, max)? {
        let len = pending.encode(plan.into_body())?;
        let encoded = pending.finish::<bool>()?;
        identity(date.response, encoded)?;
        return Ok(Complete::copied(len));
    }
    let body = plan.into_shared()?;
    let (_, encoded) = emit_head::<_, bool>(out, date, status, fields, body.len())?;
    identity(date.response, encoded)?;
    Ok(Complete::shared(body))
}

fn finish<'input, 'req, 'd, H: Into<crate::Input<'input>>, B: stage::Body<'req, 'd>>(
    plan: handoff::Plan<'req, 'd, H, B>,
) -> Result<ControlFlow<B::Deferred, Complete<'d>>, Failure> {
    let ready = match plan.stage() {
        stage::State::Body(body) => return Ok(ControlFlow::Break(body.defer())),
        stage::State::Head(input) => {
            let input = input.into();
            let body = input.try_retain().map_err(Failure::Input)?;
            match input {
                crate::Input::Borrowed(_) => Complete::Owned {
                    body,
                    cause: evidence::Cause::Borrowed,
                },
                crate::Input::Buffered(_) => Complete::Shared { body },
            }
        }
        stage::State::Ready(body) => match body {
            handoff::Ready::Empty => Complete::Inline,
            handoff::Ready::Owned(mut body) => Complete::Shared {
                body: storage::Shared::try_from(&mut body).map_err(Error::Allocation)?,
            },
            handoff::Ready::Copied(body) => {
                let (body, copy) = body.into_parts();
                Complete::Owned {
                    body,
                    cause: copy.cause,
                }
            }
            handoff::Ready::Retained(body) => Complete::Shared { body },
            handoff::Ready::Segmented(body) => Complete::Segments { body },
            handoff::Ready::Linked(body) => Complete::Linked { body },
            handoff::Ready::Fixed(body) => Complete::Static { body },
        },
    };
    Ok(ControlFlow::Continue(ready))
}
