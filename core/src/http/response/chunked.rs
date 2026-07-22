use http::StatusCode;
use o3::buffer::Shared;

use super::Response;
use super::body::Body;
use super::header::HeaderList;
use super::wire_emit::{CRLF, HeadWrite, TransferEncodingChunked, WireWriter};
use crate::http::codec::Wire;

#[derive(Clone, Debug)]
pub struct Chunked {
    status: StatusCode,
    headers: HeaderList,
    wire_headers: Shared,
    parts: Vec<Shared>,
}

impl Chunked {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn into_response(self) -> Response {
        Response {
            status: self.status,
            headers: self.headers,
            wire_headers: self.wire_headers.as_ref().to_vec(),
            body: Body::empty(),
            chunked_parts: Some(self.parts),
        }
    }

    pub(super) fn from_parts(
        status: StatusCode,
        headers: HeaderList,
        wire_headers: Shared,
        parts: Vec<Shared>,
    ) -> Self {
        Self {
            status,
            headers,
            wire_headers,
            parts,
        }
    }

    const ZERO_CHUNK: &'static [u8] = b"0\r\n\r\n";

    fn body_wire_len(&self) -> usize {
        let mut len = 0usize;
        for part in &self.parts {
            if part.is_empty() {
                continue;
            }
            let mut hex = [0u8; 16];
            let hex_n = Wire::write_hex(part.len(), &mut hex);
            len += hex_n + CRLF.len() + part.len() + CRLF.len();
        }
        len + Self::ZERO_CHUNK.len()
    }

    fn write_body(&self, out: &mut WireWriter<'_>) {
        for part in &self.parts {
            if part.is_empty() {
                continue;
            }
            let mut hex = [0u8; 16];
            let hex_n = Wire::write_hex(part.len(), &mut hex);
            out.put(&hex[..hex_n]);
            out.put(CRLF);
            out.put(part.as_ref());
            out.put(CRLF);
        }
        out.put(Self::ZERO_CHUNK);
    }

    fn head(&self) -> HeadWrite<'_, [u8], TransferEncodingChunked> {
        HeadWrite {
            status_str: self.status.as_str().as_bytes(),
            reason: self
                .status
                .canonical_reason()
                .map(str::as_bytes)
                .unwrap_or(b""),
            headers: self.wire_headers.as_ref(),
            framing: TransferEncodingChunked,
        }
    }

    pub fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        let head = self.head();
        let total = head.wire_len() + self.body_wire_len();
        if out.len() < total {
            return None;
        }

        let written = head.write(out, date);
        let mut out = WireWriter::at(out, written.len);
        self.write_body(&mut out);
        Some(out.len())
    }

    pub fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        let head = self.head();
        if out.len() < head.wire_len() {
            return None;
        }
        let written = head.write(out, date);

        let mut body = vec![0u8; self.body_wire_len()];
        self.write_body(&mut WireWriter::new(&mut body));
        Some((written.len, Shared::from(body)))
    }
}
