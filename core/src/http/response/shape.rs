use http::StatusCode;
use o3::buffer::{Pooled, Shared};

use crate::http::body_kind::ResponseKind;
use crate::http::compress::Gzip;

use super::{
    Body, Chunked, EncodedBody, EncodedResponse, FixedResponse, HeaderList, HotHeadInner,
    IntoResponseShape, MonoResponseInner, NeverStream, Serve, StaticHeaders, StaticResponseInner,
    Stream, WireHeaderFields,
};
use crate::http::Field;

pub enum Egress<S> {
    Inline { written: usize },
    Static { head: usize, body: &'static [u8] },
    Shared { head: usize, body: Shared },
    Pooled { head: usize, body: Pooled },
    Stream { head: usize, stream: S },
    Failed,
}

pub enum CacheTemplate {
    Inline {
        bytes: Vec<u8>,
        date_offset: Option<usize>,
    },
    Static {
        head: Vec<u8>,
        date_offset: Option<usize>,
        body: &'static [u8],
    },
}

impl CacheTemplate {
    pub fn configure_head(&mut self, emit_date: bool, emit_server: bool) {
        let (template, date_offset) = match self {
            Self::Inline { bytes, date_offset } => (bytes, date_offset),
            Self::Static {
                head, date_offset, ..
            } => (head, date_offset),
        };
        if let Some(offset) = *date_offset {
            use super::wire_emit::{CRLF, DATE_PREFIX, SERVER_DATE_TERMINATOR_LEN, SERVER_LINE};
            if emit_date && emit_server {
                return;
            }
            let term_start = offset - DATE_PREFIX.len() - SERVER_LINE.len();
            let term_end = term_start + SERVER_DATE_TERMINATOR_LEN;
            let mut tail = Vec::with_capacity(SERVER_DATE_TERMINATOR_LEN);
            if emit_server {
                tail.extend_from_slice(SERVER_LINE);
            }
            *date_offset = if emit_date {
                use super::wire_emit::DATE_LEN;
                tail.extend_from_slice(DATE_PREFIX);
                let offset = term_start + tail.len();
                tail.extend_from_slice(&[0u8; DATE_LEN]);
                tail.extend_from_slice(CRLF);
                Some(offset)
            } else {
                None
            };
            tail.extend_from_slice(CRLF);
            template.splice(term_start..term_end, tail);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Preparation {
    Plain,
    Compress,
    Cache,
}

pub enum Prepared<S> {
    Egress(Egress<S>),
    Cache(CacheTemplate),
}

pub struct ResponseView {
    pub status: StatusCode,
    pub headers: Shared,
    pub body: Shared,
}

pub trait ResponseSink {
    fn emit<'a, 'body, I>(&mut self, status: StatusCode, headers: I, body: Body<'body>)
    where
        I: Iterator<Item = Field<'a>>;
}

pub enum SyncShape {}
pub enum StreamShape {}

mod metadata_sealed {
    pub trait Sealed {}
}

pub trait ShapeMetadata: metadata_sealed::Sealed {
    type Kind;
    type Stream: 'static;

    const BODY_KIND: ResponseKind;
}

pub enum InlineShape {}
pub enum StaticShape {}
pub struct StreamingShape<S>(std::marker::PhantomData<fn() -> S>);

impl metadata_sealed::Sealed for InlineShape {}
impl metadata_sealed::Sealed for StaticShape {}
impl<S> metadata_sealed::Sealed for StreamingShape<S> {}

impl ShapeMetadata for InlineShape {
    type Kind = SyncShape;
    type Stream = NeverStream;

    const BODY_KIND: ResponseKind = ResponseKind::Inline;
}

impl ShapeMetadata for StaticShape {
    type Kind = SyncShape;
    type Stream = NeverStream;

    const BODY_KIND: ResponseKind = ResponseKind::Static;
}

impl<S> ShapeMetadata for StreamingShape<S>
where
    S: 'static,
{
    type Kind = StreamShape;
    type Stream = S;

    const BODY_KIND: ResponseKind = ResponseKind::Stream;
}

pub trait Shape<'req>: Sized + IntoResponseShape<'req, Shape = Self> {
    type Metadata: ShapeMetadata;

    fn prepare(
        self,
        mode: Preparation,
        gzip: Option<&mut Gzip>,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Prepared<<Self::Metadata as ShapeMetadata>::Stream>;

    fn response_view(&self) -> Option<ResponseView> {
        None
    }

    fn emit<E: ResponseSink>(self, _sink: &mut E) -> bool {
        false
    }
}

pub type ShapeKind<'req, S> = <<S as Shape<'req>>::Metadata as ShapeMetadata>::Kind;
pub type ShapeStream<'req, S> = <<S as Shape<'req>>::Metadata as ShapeMetadata>::Stream;

fn split_egress<R>(
    response: R,
    out: &mut [u8],
    date: &[u8; 29],
    write_inline: impl FnOnce(&R, &mut [u8], &[u8; 29]) -> Option<usize>,
    write_split: impl FnOnce(R, &mut [u8], &[u8; 29]) -> Option<(usize, Shared)>,
) -> Egress<NeverStream> {
    if let Some(written) = write_inline(&response, out, date) {
        return Egress::Inline { written };
    }
    match write_split(response, out, date) {
        Some((head, body)) => Egress::Shared { head, body },
        None => Egress::Failed,
    }
}

macro_rules! split_response {
    ($response:ident, $out:ident, $date:ident) => {
        split_egress(
            $response,
            $out,
            $date,
            |response, out, date| response.write_into_slice(out, date),
            |response, out, date| response.write_head_split(out, date),
        )
    };
}

macro_rules! cache_result {
    ($response:ident, inline) => {{
        let (bytes, date_offset) = $response.preserialize();
        Prepared::Cache(CacheTemplate::Inline {
            bytes,
            date_offset: Some(date_offset),
        })
    }};
    ($response:ident, unsupported) => {
        Prepared::Egress(Egress::Failed)
    };
}

macro_rules! prepare_method {
    (|$this:ident, $mode:ident, $gzip:ident, $out:ident, $date:ident| $body:block) => {
        fn prepare(
            $this,
            $mode: Preparation,
            $gzip: Option<&mut Gzip>,
            $out: &mut [u8],
            $date: &[u8; 29],
        ) -> Prepared<<Self::Metadata as ShapeMetadata>::Stream> $body
    };
}

macro_rules! prepare_split {
    ($cache:ident) => {
        prepare_method!(|self, mode, _gzip, out, date| {
            if mode == Preparation::Cache {
                return cache_result!(self, $cache);
            }
            Prepared::Egress(split_response!(self, out, date))
        });
    };
}

macro_rules! impl_identity_shape {
    (
        [$($generics:tt)*]
        $req:lifetime => $target:ty
        $(where [$($bounds:tt)*])?
    ) => {
        impl<$($generics)*> IntoResponseShape<$req> for $target
        $(where $($bounds)*)?
        {
            type Shape = Self;

            fn into_response_shape(self) -> Self::Shape {
                self
            }
        }
    };
}

impl_identity_shape! {
    ['req, const N: usize, S]
    'req => FixedResponse<'req, N, S>
    where [S: StaticHeaders]
}

impl<'req, const N: usize, S> Shape<'req> for FixedResponse<'req, N, S>
where
    S: StaticHeaders,
{
    type Metadata = InlineShape;

    prepare_method!(|self, mode, gzip, out, date| {
        if mode == Preparation::Cache {
            return cache_result!(self, inline);
        }
        if mode == Preparation::Compress
            && !self.has_content_encoding()
            && !self.body_ref().is_empty()
            && let Some(body) = gzip.and_then(|gzip| gzip.encode(self.body_ref()))
        {
            let body_len = body.len();
            let egress = match self.write_gzip_head(out, date, body_len) {
                Some(head) => Egress::Pooled { head, body },
                None => Egress::Failed,
            };
            return Prepared::Egress(egress);
        }
        Prepared::Egress(split_response!(self, out, date))
    });

    fn response_view(&self) -> Option<ResponseView> {
        Some(ResponseView {
            status: self.status(),
            headers: self.wire_headers(),
            body: self.body.clone(),
        })
    }

    fn emit<E: ResponseSink>(self, sink: &mut E) -> bool {
        let Self { status, head, body } = self;
        sink.emit(status, head.fields(), Body::Shared(body));
        true
    }
}

impl_identity_shape! {
    ['req, B, const N: usize, S]
    'req => EncodedResponse<'req, B, N, S>
    where [
        B: EncodedBody,
        S: StaticHeaders,
    ]
}

impl<'req, B, const N: usize, S> Shape<'req> for EncodedResponse<'req, B, N, S>
where
    B: EncodedBody,
    S: StaticHeaders,
{
    type Metadata = InlineShape;

    prepare_method!(|self, mode, _gzip, out, date| {
        if mode == Preparation::Cache {
            let Some((bytes, date_offset)) = self.preserialize() else {
                return Prepared::Egress(Egress::Failed);
            };
            return Prepared::Cache(CacheTemplate::Inline {
                bytes,
                date_offset: Some(date_offset),
            });
        }
        Prepared::Egress(split_response!(self, out, date))
    });

    fn response_view(&self) -> Option<ResponseView> {
        Some(ResponseView {
            status: self.status(),
            headers: self.wire_headers(),
            body: self.encoded_body()?,
        })
    }

    fn emit<E: ResponseSink>(self, sink: &mut E) -> bool {
        let Self {
            status,
            head,
            body,
            body_len,
        } = self;
        let mut encoded = vec![0; body_len];
        if body.encode_into(&mut encoded).is_err() {
            return false;
        }
        sink.emit(status, head.fields(), Body::Owned(encoded));
        true
    }
}

impl_identity_shape! {
    ['req, const N: usize]
    'req => MonoResponseInner<'req, N>
}

impl<'req, const N: usize> Shape<'req> for MonoResponseInner<'req, N> {
    type Metadata = InlineShape;

    prepare_split!(unsupported);

    fn response_view(&self) -> Option<ResponseView> {
        Some(ResponseView {
            status: self.status(),
            headers: self.wire_headers(),
            body: self.body.clone().into_shared(),
        })
    }

    fn emit<E: ResponseSink>(self, sink: &mut E) -> bool {
        let Self {
            status,
            headers,
            head,
            body,
        } = self;
        let dynamic = match headers.as_deref() {
            Some(headers) => headers,
            None => HeaderList::empty_static(),
        };
        match head {
            HotHeadInner::Wire(wire) => {
                sink.emit(
                    status,
                    WireHeaderFields::new(wire.as_ref()).chain(dynamic.fields()),
                    body,
                );
            }
            HotHeadInner::Direct(head) => {
                sink.emit(status, head.fields().chain(dynamic.fields()), body);
            }
        }
        true
    }
}

impl_identity_shape! {
    ['req, const N: usize, S]
    'req => StaticResponseInner<'req, N, S>
    where [S: StaticHeaders]
}

impl<'req, const N: usize, S> Shape<'req> for StaticResponseInner<'req, N, S>
where
    S: StaticHeaders,
{
    type Metadata = StaticShape;

    prepare_method!(|self, mode, _gzip, out, date| {
        if mode == Preparation::Cache {
            let (head, date_offset, body) = self.preserialize_static();
            return Prepared::Cache(CacheTemplate::Static {
                head,
                date_offset: Some(date_offset),
                body,
            });
        }
        Prepared::Egress(match self.write_head_only(out, date) {
            Some((head, body)) => Egress::Static { head, body },
            None => Egress::Failed,
        })
    });

    fn response_view(&self) -> Option<ResponseView> {
        Some(ResponseView {
            status: self.status(),
            headers: self.wire_headers(),
            body: Shared::from_static(self.body_ref()),
        })
    }

    fn emit<E: ResponseSink>(self, sink: &mut E) -> bool {
        let Self { status, head, body } = self;
        sink.emit(status, head.fields(), Body::StaticSlice(body));
        true
    }
}

impl_identity_shape! {
    ['req]
    'req => Chunked
}

impl<'req> Shape<'req> for Chunked {
    type Metadata = InlineShape;

    prepare_split!(unsupported);
}

impl_identity_shape! {
    ['req, S]
    'req => Stream<S>
    where [S: 'static]
}

impl<'req, S> Shape<'req> for Stream<S>
where
    S: 'static,
{
    type Metadata = StreamingShape<S>;

    prepare_method!(|self, mode, _gzip, out, date| {
        if mode == Preparation::Cache {
            return Prepared::Egress(Egress::Failed);
        }
        Prepared::Egress(match self.write_head_stream(out, date) {
            Some((head, stream)) => Egress::Stream { head, stream },
            None => Egress::Failed,
        })
    });
}

impl_identity_shape! {
    ['req, const N: usize]
    'req => Serve<'req, N>
}

impl<'req, const N: usize> Shape<'req> for Serve<'req, N> {
    type Metadata = InlineShape;

    prepare_method!(|self, mode, gzip, out, date| {
        match self {
            Self::Fixed(response) => response.prepare(mode, gzip, out, date),
            Self::Mono(response) => response.prepare(mode, gzip, out, date),
            Self::Chunked(response) => response.prepare(mode, gzip, out, date),
        }
    });

    fn response_view(&self) -> Option<ResponseView> {
        match self {
            Self::Fixed(response) => response.response_view(),
            Self::Mono(response) => response.response_view(),
            Self::Chunked(_) => None,
        }
    }

    fn emit<E: ResponseSink>(self, sink: &mut E) -> bool {
        match self {
            Self::Fixed(response) => response.emit(sink),
            Self::Mono(response) => response.emit(sink),
            Self::Chunked(_) => false,
        }
    }
}
