use o3::buffer::bytes::scope;
use o3::buffer::write::Slice;
use sark_http::{
    field,
    h1::head::{
        line::Version,
        response::{Date, Server},
    },
    owner::{handoff, handoff::stage},
    payload::{
        compress::Compression,
        fixed::Body,
        response::{self, Content, bounded, dispatch, exact, headers, status::Code},
    },
};

use crate::wire::{
    cache,
    output::Admit,
    response::{Context, framing},
};

mod cached;
pub(super) mod error;
mod gzip;
mod head;
pub(super) mod written;

use error::Error;
use gzip::{compress_body, compress_bounded, compress_exact};
use head::{Layout, Pending, emit_framed_head, emit_head};
use written::Written;

pub(super) enum Mode<'a, C: Compression> {
    Plain,
    Compress(&'a mut C),
    Cache(cache::Cache<'a>),
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

    fn empty<H: field::Source>(
        self,
        status: Code,
        fields: &H,
        frame: framing::Empty,
    ) -> Result<Written<'d, O::Deferred, Q::Selection>, Error> {
        match self.mode {
            Mode::Cache(cache) => Written::cached(
                self.out,
                self.date.value,
                cached::empty(status, fields, frame, self.date)?,
                cache,
            ),
            Mode::Plain | Mode::Compress(_) => {
                emit_framed_head::<_, _, ()>(self.out, self.date, status, fields, frame)?;
                Ok(Written::Body {
                    body: handoff::Ready::Empty,
                })
            }
        }
    }
}

impl<'req, 'd: 'req, Q: scope::Source, O: stage::Body<'req, 'd>, C: Compression, A>
    dispatch::Accept<'d> for Sink<'_, 'req, 'd, Q, O, C, A>
{
    type Output = Written<'d, O::Deferred, Q::Selection>;
    type Error = Error;

    fn fixed<H, B>(self, status: Code, fields: H, body: B) -> Result<Self::Output, Self::Error>
    where
        H: headers::Source,
        B: response::Body<'d>,
    {
        let fields = headers::Fields::new(fields);
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
            Mode::Cache(cache) => Written::cached(
                out,
                date.value,
                cached::body(status, &fields, body, date)?,
                cache,
            ),
            Mode::Compress(gzip) => compress_body(out, date, status, &fields, body, gzip, slot),
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
        H: headers::Source,
        'body: 'd,
    {
        let fields = headers::Fields::new(fields);
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
            Mode::Cache(cache) => Written::cached(
                out,
                date.value,
                cached::body(status, &fields, body, date)?,
                cache,
            ),
            Mode::Plain | Mode::Compress(_) => {
                emit_head::<_, ()>(out, date, status, &fields, body.len())?;
                stage(slot, body)
            }
        }
    }

    fn exact<H, B>(self, status: Code, fields: H, body: B) -> Result<Self::Output, Self::Error>
    where
        H: headers::Source,
        B: exact::TryBody,
    {
        let fields = headers::Fields::new(fields);
        if let Some(frame) = self.omission(status)? {
            return self.empty(status, &fields, frame);
        }
        let plan = exact::Plan::try_new(body).map_err(|error| Error::Size(error.discard()))?;
        let Self {
            out,
            date,
            mode,
            slot,
            ..
        } = self;
        match mode {
            Mode::Cache(cache) => Written::cached(
                out,
                date.value,
                cached::exact(status, &fields, plan, date)?,
                cache,
            ),
            Mode::Compress(gzip) => compress_exact(out, date, status, &fields, plan, gzip, slot),
            Mode::Plain => finish_exact(out, date, status, &fields, plan, slot),
        }
    }

    fn bounded<H, B>(self, status: Code, fields: H, body: B) -> Result<Self::Output, Self::Error>
    where
        H: headers::Source,
        B: bounded::Body,
    {
        let fields = headers::Fields::new(fields);
        if let Some(frame) = self.omission(status)? {
            return self.empty(status, &fields, frame);
        }
        let plan = bounded::Plan::new(body);
        let Self {
            out,
            date,
            mode,
            slot,
            ..
        } = self;
        match mode {
            Mode::Cache(cache) => Written::cached(
                out,
                date.value,
                cached::bounded(status, &fields, plan, date)?,
                cache,
            ),
            Mode::Compress(gzip) => compress_bounded(out, date, status, &fields, plan, gzip, slot),
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
        stream: S,
    ) -> Result<Self::Stream<S>, Self::Error>
    where
        H: headers::Source,
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
        stream: S,
    ) -> Result<Self::Stream<S>, Self::Error>
    where
        H: headers::Source,
    {
        let fields = headers::Fields::new(fields);
        let frame = self.omission(status)?;
        let frame = match self.admit {
            Admit::Head => framing::Empty::Unframed,
            Admit::Body(admit) => match frame {
                Some(frame) => frame,
                None => {
                    if self.date.response.version() == Version::Ten {
                        return Err(Error::StreamingVersion(Version::Ten));
                    }
                    emit_framed_head::<_, _, ()>(
                        self.out,
                        self.date,
                        status,
                        &fields,
                        framing::Chunked,
                    )?;
                    return Ok(Some((admit, stream)));
                }
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
    H: field::Source,
    B: response::Body<'d>,
{
    let (head, ()) = emit_head(out.reborrow(), date, status, fields, body.len())?;
    let inline = if B::INLINE { body.as_slice() } else { None };
    if let Some(raw) = inline {
        head.checked_add(raw.len()).ok_or(Error::Overflow {
            base: head,
            added: raw.len(),
        })?;
        if o3::buffer::write::Sink::write_slice(&mut out, raw).is_ok() {
            return Ok(Written::inline(raw.len()));
        }
    }
    stage(slot, body)
}

fn finish_exact<
    'req,
    'd,
    H: field::Source,
    B: exact::TryBody,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
>(
    mut out: Slice<'_, &mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    plan: exact::Plan<B>,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<Written<'d, O::Deferred, Q::Selection>, Error> {
    let len = plan.len();
    let (head, ()) = emit_head(out.reborrow(), date, status, fields, len)?;
    place_exact(out, head, plan, slot)
}

fn place_exact<'req, 'd, B: exact::TryBody, Q: scope::Source, O: stage::Body<'req, 'd>>(
    mut out: Slice<'_, &mut usize>,
    head: usize,
    plan: exact::Plan<B>,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<Written<'d, O::Deferred, Q::Selection>, Error> {
    let len = plan.len();
    head.checked_add(len).ok_or(Error::Overflow {
        base: head,
        added: len,
    })?;
    if len <= out.remaining() {
        plan.encode(&mut out)?;
        return Ok(Written::inline(len));
    }
    let body = plan.into_shared()?;
    stage(slot, body)
}

fn finish_bounded<
    'out,
    'req,
    'd,
    H: field::Source,
    B: bounded::Body,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
>(
    mut out: Slice<'out, &'out mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    plan: bounded::Plan<B>,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<Written<'d, O::Deferred, Q::Selection>, Error> {
    let max = plan.max_len();
    let layout = Layout::new(date, status, fields, max)?;
    if let Some(mut pending) = Pending::place(&layout, &mut out) {
        let len = pending.encode(plan)?;
        pending.finish()?;
        return Ok(Written::inline(len));
    }
    let body = plan.into_shared()?;
    emit_head::<_, ()>(out, date, status, fields, body.len())?;
    stage(slot, body)
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
