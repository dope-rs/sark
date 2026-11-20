use o3::buffer::bytes::scope;
use o3::buffer::write::Slice;
use sark_http::{
    field,
    h1::head::{
        line::Version,
        request::Coding,
        response::{Date, Server},
    },
    owner::{handoff, handoff::stage},
    payload::{
        compress::{self, Compression},
        fixed::Body,
        response::{self, Content, Encode, dispatch, encoded, headers, status::Code},
    },
};

use crate::wire::{
    output::Admit,
    response::{Context, framing},
};

pub(super) mod error;
mod gzip;
mod head;
pub(super) mod written;

use error::Error;
use gzip::{compress_body, compress_bounded, compress_exact};
use head::{Pending, emit_framed_head, emit_head};
use written::Written;

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
    A = (),
> {
    out: Slice<'a, &'a mut usize>,
    date: Headers<'a>,
    mode: Mode<'a, C>,
    slot: handoff::Slot<'req, 'd, Q, O>,
    admit: A,
}

impl<'a, 'req, 'd, Q: scope::Source, O: stage::Body<'req, 'd>, C: Compression>
    Sink<'a, 'req, 'd, Q, O, C>
{
    pub(super) const fn new(
        out: Slice<'a, &'a mut usize>,
        date: &'a [u8; 29],
        response: Context,
        mode: Mode<'a, C>,
        slot: handoff::Slot<'req, 'd, Q, O>,
    ) -> Self {
        Self {
            out,
            date: Headers {
                value: date,
                dated: Date::Include,
                server: Server::Include,
                response,
            },
            mode,
            slot,
            admit: (),
        }
    }
}

impl<'a, 'req, 'd, Q: scope::Source, O: stage::Body<'req, 'd>, C: Compression, A>
    Sink<'a, 'req, 'd, Q, O, C, A>
{
    pub(super) fn with_policy(mut self, policy: crate::route::Policy) -> Self {
        self.date.dated = policy.date;
        self.date.server = policy.server;
        self
    }

    pub(super) fn with_admit<T>(self, admit: T) -> Sink<'a, 'req, 'd, Q, O, C, T> {
        Sink {
            out: self.out,
            date: self.date,
            mode: self.mode,
            slot: self.slot,
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
    ) -> Result<Written<'d, O::Deferred, Q::Selection>, Error> {
        emit_framed_head::<_, _, ()>(self.out, self.date, status, fields, frame)?;
        Ok(Written::Body {
            body: handoff::Ready::Empty,
        })
    }
}

impl<'req, 'd: 'req, Q: scope::Source, O: stage::Body<'req, 'd>, C: Compression, A>
    dispatch::Accept<'d> for Sink<'_, 'req, 'd, Q, O, C, A>
{
    type Output = Written<'d, O::Deferred, Q::Selection>;
    type Error = Error;

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
        let (_, encoded) = emit_head::<_, bool>(self.out, self.date, status, &fields, body.len())?;
        let written = stage(self.slot, body)?;
        identity(self.date.response, encoded)?;
        Ok(written)
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
        let Self {
            out,
            date,
            mode,
            slot,
            ..
        } = self;
        match mode {
            Mode::Compress { gzip, required } if EXACT => {
                compress_exact(out, date, status, &fields, plan, gzip, required, slot)
            }
            Mode::Compress { gzip, required } => {
                compress_bounded(out, date, status, &fields, plan, gzip, required, slot)
            }
            Mode::Plain if EXACT => finish_exact(out, date, status, &fields, plan, slot),
            Mode::Plain => finish_bounded(out, date, status, &fields, plan, slot),
        }
    }
}

impl<'req, 'd: 'req, Q: scope::Source, O: stage::Body<'req, 'd>, C: Compression> dispatch::Sink<'d>
    for Sink<'_, 'req, 'd, Q, O, C>
{
    type Stream<S> = Option<S>;

    fn stream<H, S>(
        self,
        status: Code,
        fields: H,
        stream: Option<S>,
    ) -> Result<Self::Stream<S>, Self::Error>
    where
        H: field::Source<headers::Application>,
    {
        let admit = match self.date.response.method() {
            response::Method::Head => Admit::Head,
            response::Method::Other | response::Method::Connect => Admit::Body(()),
        };
        self.with_admit(admit)
            .stream(status, fields, stream)
            .map(|body| body.map(|((), stream)| stream))
    }
}

impl<'req, 'd: 'req, Q: scope::Source, O: stage::Body<'req, 'd>, C: Compression, A>
    dispatch::Sink<'d> for Sink<'_, 'req, 'd, Q, O, C, Admit<A>>
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
            Admit::Head => framing::Empty::Unframed,
            Admit::Body(admit) => match frame {
                Some(frame) => frame,
                None => match stream {
                    None => framing::Empty::Zero,
                    Some(stream) => {
                        if self.date.response.version() == Version::Ten {
                            return Err(Error::StreamingVersion(Version::Ten));
                        }
                        let (_, encoded) = emit_framed_head::<_, _, bool>(
                            self.out,
                            self.date,
                            status,
                            &fields,
                            framing::Chunked,
                        )?;
                        if matches!(self.mode, Mode::Compress { required: true, .. }) {
                            return Err(Error::Compression(compress::Error::Unavailable));
                        }
                        identity(self.date.response, encoded)?;
                        return Ok(Some((admit, stream)));
                    }
                },
            },
        };
        emit_framed_head::<_, _, ()>(self.out, self.date, status, &fields, frame)?;
        Ok(None)
    }
}

fn plain_body<'req, 'd, H, B, Q: scope::Source, O: stage::Body<'req, 'd>>(
    mut out: Slice<'_, &mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    body: B,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<Written<'d, O::Deferred, Q::Selection>, Error>
where
    H: field::Source<headers::Application>,
    B: response::Body<'d>,
{
    let (head, encoded) = emit_head::<_, bool>(out.reborrow(), date, status, fields, body.len())?;
    let inline = if B::INLINE { body.as_slice() } else { None };
    if let Some(raw) = inline {
        head.checked_add(raw.len()).ok_or(Error::Overflow {
            base: head,
            added: raw.len(),
        })?;
        if o3::buffer::write::Sink::write_slice(&mut out, raw).is_ok() {
            identity(date.response, encoded)?;
            return Ok(Written::inline(raw.len()));
        }
    }
    let written = stage(slot, body)?;
    identity(date.response, encoded)?;
    Ok(written)
}

fn finish_exact<
    'req,
    'd,
    H: field::Source<headers::Application>,
    B: Encode,
    const EXACT: bool,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
>(
    mut out: Slice<'_, &mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    plan: encoded::Plan<B, EXACT>,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<Written<'d, O::Deferred, Q::Selection>, Error> {
    let len = plan.limit();
    let (head, encoded) = emit_head::<_, bool>(out.reborrow(), date, status, fields, len)?;
    let written = place_exact(out, head, plan, slot)?;
    identity(date.response, encoded)?;
    Ok(written)
}

fn place_exact<
    'req,
    'd,
    B: Encode,
    const EXACT: bool,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
>(
    mut out: Slice<'_, &mut usize>,
    head: usize,
    plan: encoded::Plan<B, EXACT>,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<Written<'d, O::Deferred, Q::Selection>, Error> {
    let len = plan.limit();
    head.checked_add(len).ok_or(Error::Overflow {
        base: head,
        added: len,
    })?;
    if len <= out.remaining() {
        plan.write(&mut out)?;
        return Ok(Written::inline(len));
    }
    let body = plan.into_shared()?;
    stage(slot, body)
}

fn finish_bounded<
    'out,
    'req,
    'd,
    H: field::Source<headers::Application>,
    B: Encode,
    const EXACT: bool,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
>(
    mut out: Slice<'out, &'out mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    plan: encoded::Plan<B, EXACT>,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<Written<'d, O::Deferred, Q::Selection>, Error> {
    let max = plan.limit();
    if let Some(mut pending) = Pending::try_place(&mut out, date, status, fields, max)? {
        let len = pending.encode(plan.into_body())?;
        let encoded = pending.finish::<bool>()?;
        identity(date.response, encoded)?;
        return Ok(Written::inline(len));
    }
    let body = plan.into_shared()?;
    let (_, encoded) = emit_head::<_, bool>(out, date, status, fields, body.len())?;
    let written = stage(slot, body)?;
    identity(date.response, encoded)?;
    Ok(written)
}

fn stage<'req, 'd, B, Q: scope::Source, O: stage::Body<'req, 'd>>(
    slot: handoff::Slot<'req, 'd, Q, O>,
    body: B,
) -> Result<Written<'d, O::Deferred, Q::Selection>, Error>
where
    B: response::Body<'d>,
{
    let plan = response::Body::try_resolve(body, &slot)?;
    Ok(staged(plan.stage()))
}

fn staged<'req, 'd, H, B: stage::Body<'req, 'd>>(
    body: stage::State<'d, H, B>,
) -> Written<'d, B::Deferred, H> {
    match body {
        stage::State::Body(body) => Written::RequestBody { body: body.defer() },
        stage::State::Ready(body) => Written::Body { body },
        stage::State::Head(input) => Written::Head { input },
    }
}
