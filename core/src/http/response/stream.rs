use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use http::StatusCode;
use o3::buffer::{Owned, Shared};

use super::wire_emit::{HeadWrite, TransferEncodingChunked};

pub const CHUNK_TERMINATOR: &[u8; 5] = b"0\r\n\r\n";

pub struct Stream<S>
where
    S: Future<Output = Option<Shared>> + 'static,
{
    status: StatusCode,
    wire_headers: Shared,
    stream: S,
}

pub struct IterStream<I> {
    iter: I,
}

impl<I> Future for IterStream<I>
where
    I: Iterator<Item = Shared> + Unpin + 'static,
{
    type Output = Option<Shared>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Shared>> {
        Poll::Ready(self.get_mut().iter.next())
    }
}

impl<S> Stream<S>
where
    S: Future<Output = Option<Shared>> + 'static,
{
    pub fn new(stream: S) -> Self {
        Self {
            status: StatusCode::OK,
            wire_headers: Shared::new(),
            stream,
        }
    }

    pub fn header(mut self, name: &[u8], value: &[u8]) -> Self {
        let mut buf =
            Owned::with_capacity(self.wire_headers.len() + name.len() + 2 + value.len() + 2);
        buf.extend_from_slice(self.wire_headers.as_ref());
        buf.extend_from_slice(name);
        buf.extend_from_slice(b": ");
        buf.extend_from_slice(value);
        buf.extend_from_slice(b"\r\n");
        self.wire_headers = buf.freeze();
        self
    }

    pub fn write_head_stream(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, S)> {
        let status_str = self.status.as_str().as_bytes();
        let reason = self
            .status
            .canonical_reason()
            .map(str::as_bytes)
            .unwrap_or(b"");

        let head = HeadWrite {
            status_str,
            reason,
            headers: self.wire_headers.as_ref(),
            framing: TransferEncodingChunked,
        };
        if out.len() < head.wire_len() {
            return None;
        }

        let mut off = 0usize;
        head.write(out, &mut off, date);
        Some((off, self.stream))
    }
}

impl<II> Stream<IterStream<II>>
where
    II: Iterator<Item = Shared> + Unpin + 'static,
{
    pub fn from_chunks<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Shared, IntoIter = II>,
    {
        Self::new(IterStream {
            iter: iter.into_iter(),
        })
    }
}

impl<S> super::IntoServeResponse<'static> for Stream<S>
where
    S: Future<Output = Option<Shared>> + 'static,
{
    fn into_serve_response(self) -> super::ServeInner<'static> {
        unreachable!(
            "Stream::into_serve_response — Stream routes \
             route through Stream::write_head_stream"
        )
    }
}
