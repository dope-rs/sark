use std::pin::Pin;
use std::task::Poll;

use dope_fiber::{Context, Fiber};
use http::StatusCode;
use o3::buffer::Shared;

use super::wire_emit::{HeadWrite, TransferEncodingChunked};

pub const CHUNK_TERMINATOR: &[u8; 5] = b"0\r\n\r\n";

pub struct Stream<S> {
    status: StatusCode,
    wire_headers: Vec<Shared>,
    stream: S,
}

pub struct IterStream<I> {
    iter: I,
}

impl<'d, I> Fiber<'d> for IterStream<I>
where
    I: Iterator<Item = Shared> + Unpin + 'static,
{
    type Output = Option<Shared>;
    fn poll(self: Pin<&mut Self>, _cx: Pin<&mut Context<'_, 'd>>) -> Poll<Option<Shared>> {
        Poll::Ready(self.get_mut().iter.next())
    }
}

impl<S> Stream<S> {
    pub fn new(stream: S) -> Self {
        Self {
            status: StatusCode::OK,
            wire_headers: Vec::new(),
            stream,
        }
    }

    pub fn header(mut self, name: &[u8], value: &[u8]) -> Self {
        let mut buf = Vec::new();
        buf.extend_from_slice(name);
        buf.extend_from_slice(b": ");
        buf.extend_from_slice(value);
        buf.extend_from_slice(b"\r\n");
        self.wire_headers.push(Shared::from(buf));
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
            headers: self.wire_headers.as_slice(),
            framing: TransferEncodingChunked,
        };
        if out.len() < head.wire_len() {
            return None;
        }

        let written = head.write(out, date);
        Some((written.len, self.stream))
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
