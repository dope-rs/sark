use dope_fiber::{Context, Fiber};
use http::StatusCode;
use o3::buffer::Shared;

use super::{
    Chunked, EncodedBody, EncodedResponseInner, FixedResponseInner, MonoResponseInner, ServeInner,
    StaticResponseInner, Stream,
};

pub struct NeverStream(std::marker::PhantomData<()>);

impl<'d> Fiber<'d> for NeverStream {
    type Output = Option<Shared>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: std::pin::Pin<&mut Context<'_, 'd>>,
    ) -> std::task::Poll<Self::Output> {
        unreachable!("NeverStream polled — non-Stream Shape")
    }
}

pub trait Shape<'req>: Sized {
    type StreamInner: 'static;

    fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        let _ = (out, date);
        unreachable!("Shape::write_into_slice called on non-Fixed shape")
    }

    fn preserialize(&self) -> (Vec<u8>, usize) {
        unreachable!("Shape::preserialize called on non-Fixed shape")
    }

    fn write_head_only(&self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, &'static [u8])> {
        let _ = (out, date);
        unreachable!("Shape::write_head_only called on non-Static-body shape")
    }

    fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        let _ = (out, date);
        None
    }

    fn preserialize_static(&self) -> Option<(Vec<u8>, usize, &'static [u8])> {
        None
    }

    fn write_head_stream(
        self,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Option<(usize, Self::StreamInner)> {
        let _ = (out, date);
        unreachable!("Shape::write_head_stream called on non-Stream-body shape")
    }

    fn body_for_gzip(&self) -> Option<&[u8]> {
        None
    }

    fn write_gzip_head(self, out: &mut [u8], date: &[u8; 29], body_len: usize) -> Option<usize> {
        let _ = (out, date, body_len);
        unreachable!("Shape::write_gzip_head called on shape without body_for_gzip")
    }

    fn status(&self) -> StatusCode {
        unreachable!("Shape::status called on shape without a status line")
    }

    fn body_bytes(&self) -> &[u8] {
        &[]
    }

    fn headers_wire(&self) -> Shared {
        Shared::new()
    }
}

impl<'req, const N: usize> Shape<'req> for FixedResponseInner<'req, N> {
    type StreamInner = NeverStream;

    fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        FixedResponseInner::write_into_slice(self, out, date)
    }

    fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        FixedResponseInner::write_head_split(self, out, date)
    }

    fn preserialize(&self) -> (Vec<u8>, usize) {
        FixedResponseInner::preserialize(self)
    }

    fn body_for_gzip(&self) -> Option<&[u8]> {
        if self.has_content_encoding() {
            None
        } else {
            Some(self.body_ref())
        }
    }

    fn write_gzip_head(self, out: &mut [u8], date: &[u8; 29], body_len: usize) -> Option<usize> {
        FixedResponseInner::write_gzip_head(self, out, date, body_len)
    }

    fn status(&self) -> StatusCode {
        FixedResponseInner::status(self)
    }

    fn body_bytes(&self) -> &[u8] {
        self.body_ref()
    }

    fn headers_wire(&self) -> Shared {
        self.wire_headers()
    }
}

impl<'req, B, const N: usize> Shape<'req> for EncodedResponseInner<'req, B, N>
where
    B: EncodedBody,
{
    type StreamInner = NeverStream;

    fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        EncodedResponseInner::write_into_slice(self, out, date)
    }

    fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        EncodedResponseInner::write_head_split(self, out, date)
    }

    fn preserialize(&self) -> (Vec<u8>, usize) {
        EncodedResponseInner::preserialize(self)
    }

    fn status(&self) -> StatusCode {
        EncodedResponseInner::status(self)
    }

    fn headers_wire(&self) -> Shared {
        EncodedResponseInner::wire_headers(self)
    }
}

impl<'req, const N: usize> Shape<'req> for MonoResponseInner<'req, N> {
    type StreamInner = NeverStream;

    fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        MonoResponseInner::write_into_slice(self, out, date)
    }

    fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        MonoResponseInner::write_head_split(self, out, date)
    }
}

impl<'req, const N: usize> Shape<'req> for StaticResponseInner<'req, N> {
    type StreamInner = NeverStream;

    fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        StaticResponseInner::write_into_slice(self, out, date)
    }

    fn write_head_only(&self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, &'static [u8])> {
        StaticResponseInner::write_head_only(self, out, date)
    }

    fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        StaticResponseInner::write_head_split(self, out, date)
    }

    fn preserialize_static(&self) -> Option<(Vec<u8>, usize, &'static [u8])> {
        Some(StaticResponseInner::preserialize_static(self))
    }

    fn status(&self) -> StatusCode {
        StaticResponseInner::status(self)
    }

    fn body_bytes(&self) -> &[u8] {
        self.body_ref()
    }

    fn headers_wire(&self) -> Shared {
        StaticResponseInner::wire_headers(self)
    }
}

impl<'req> Shape<'req> for Chunked {
    type StreamInner = NeverStream;

    fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        Self::write_into_slice(self, out, date)
    }

    fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        Chunked::write_head_split(self, out, date)
    }
}

impl<'req, S> Shape<'req> for Stream<S>
where
    S: 'static,
{
    type StreamInner = S;

    fn write_head_stream(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, S)> {
        Stream::<S>::write_head_stream(self, out, date)
    }
}

impl<'req, const N: usize> Shape<'req> for ServeInner<'req, N> {
    type StreamInner = NeverStream;

    fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        match self {
            Self::Fixed(f) => f.write_into_slice(out, date),
            Self::Mono(m) => m.write_into_slice(out, date),
            Self::Chunked(c) => c.write_into_slice(out, date),
        }
    }

    fn preserialize(&self) -> (Vec<u8>, usize) {
        match self {
            Self::Fixed(f) => f.preserialize(),
            Self::Mono(_) | Self::Chunked(_) => {
                unreachable!("Shape::preserialize: STATIC_RESPONSE route returned non-Fixed")
            }
        }
    }

    fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        match self {
            Self::Mono(m) => MonoResponseInner::write_head_split(m, out, date),
            Self::Chunked(c) => Chunked::write_head_split(c, out, date),
            Self::Fixed(f) => FixedResponseInner::write_head_split(f, out, date),
        }
    }

    fn body_for_gzip(&self) -> Option<&[u8]> {
        match self {
            Self::Fixed(f) if !f.has_content_encoding() => Some(f.body_ref()),
            _ => None,
        }
    }

    fn write_gzip_head(self, out: &mut [u8], date: &[u8; 29], body_len: usize) -> Option<usize> {
        match self {
            Self::Fixed(f) => FixedResponseInner::write_gzip_head(f, out, date, body_len),
            _ => unreachable!("write_gzip_head on non-Fixed ServeInner"),
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::Fixed(f) => f.status(),
            Self::Mono(m) => m.status(),
            Self::Chunked(_) => StatusCode::OK,
        }
    }

    fn body_bytes(&self) -> &[u8] {
        match self {
            Self::Fixed(f) => f.body_ref(),
            Self::Mono(_) | Self::Chunked(_) => &[],
        }
    }

    fn headers_wire(&self) -> Shared {
        match self {
            Self::Fixed(f) => f.wire_headers(),
            Self::Mono(_) | Self::Chunked(_) => Shared::new(),
        }
    }
}
