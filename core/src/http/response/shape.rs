use http::StatusCode;
use o3::buffer::{Pooled, Shared};

use crate::http::compress::Gzip;

use super::{
    Chunked, EncodedBody, EncodedResponse, FixedResponse, MonoResponseInner, NeverStream, Serve,
    StaticResponseInner, Stream,
};

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
            if emit_date && emit_server {
                return;
            }
            let term_start =
                offset - super::wire_emit::DATE_PREFIX.len() - super::wire_emit::SERVER_LINE.len();
            let term_end = term_start + super::wire_emit::SERVER_DATE_TERMINATOR_LEN;
            let mut tail = Vec::with_capacity(super::wire_emit::SERVER_DATE_TERMINATOR_LEN);
            if emit_server {
                tail.extend_from_slice(super::wire_emit::SERVER_LINE);
            }
            *date_offset = if emit_date {
                tail.extend_from_slice(super::wire_emit::DATE_PREFIX);
                let offset = term_start + tail.len();
                tail.extend_from_slice(&[0u8; super::wire_emit::DATE_LEN]);
                tail.extend_from_slice(super::wire_emit::CRLF);
                Some(offset)
            } else {
                None
            };
            tail.extend_from_slice(super::wire_emit::CRLF);
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

pub trait Shape<'req>: Sized {
    type StreamInner: 'static;

    fn prepare(
        self,
        mode: Preparation,
        gzip: Option<&mut Gzip>,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Prepared<Self::StreamInner>;

    fn response_view(&self) -> Option<ResponseView> {
        None
    }
}

impl<'req, const N: usize> Shape<'req> for FixedResponse<'req, N> {
    type StreamInner = NeverStream;

    fn prepare(
        self,
        mode: Preparation,
        gzip: Option<&mut Gzip>,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Prepared<Self::StreamInner> {
        if mode == Preparation::Cache {
            let (bytes, date_offset) = self.preserialize();
            return Prepared::Cache(CacheTemplate::Inline {
                bytes,
                date_offset: Some(date_offset),
            });
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
        Prepared::Egress(fixed_egress(self, out, date))
    }

    fn response_view(&self) -> Option<ResponseView> {
        Some(ResponseView {
            status: self.status(),
            headers: self.wire_headers(),
            body: self.body.clone(),
        })
    }
}

impl<'req, B, const N: usize> Shape<'req> for EncodedResponse<'req, B, N>
where
    B: EncodedBody,
{
    type StreamInner = NeverStream;

    fn prepare(
        self,
        mode: Preparation,
        _gzip: Option<&mut Gzip>,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Prepared<Self::StreamInner> {
        if mode == Preparation::Cache {
            let (bytes, date_offset) = self.preserialize();
            return Prepared::Cache(CacheTemplate::Inline {
                bytes,
                date_offset: Some(date_offset),
            });
        }
        let egress = if let Some(written) = self.write_into_slice(out, date) {
            Egress::Inline { written }
        } else {
            match self.write_head_split(out, date) {
                Some((head, body)) => Egress::Shared { head, body },
                None => Egress::Failed,
            }
        };
        Prepared::Egress(egress)
    }

    fn response_view(&self) -> Option<ResponseView> {
        Some(ResponseView {
            status: self.status(),
            headers: self.wire_headers(),
            body: self.encoded_body(),
        })
    }
}

impl<'req, const N: usize> Shape<'req> for MonoResponseInner<'req, N> {
    type StreamInner = NeverStream;

    fn prepare(
        self,
        mode: Preparation,
        _gzip: Option<&mut Gzip>,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Prepared<Self::StreamInner> {
        if mode == Preparation::Cache {
            return Prepared::Egress(Egress::Failed);
        }
        let egress = if let Some(written) = self.write_into_slice(out, date) {
            Egress::Inline { written }
        } else {
            match self.write_head_split(out, date) {
                Some((head, body)) => Egress::Shared { head, body },
                None => Egress::Failed,
            }
        };
        Prepared::Egress(egress)
    }

    fn response_view(&self) -> Option<ResponseView> {
        Some(ResponseView {
            status: self.status(),
            headers: self.wire_headers(),
            body: self.body.clone().into_shared(),
        })
    }
}

impl<'req, const N: usize> Shape<'req> for StaticResponseInner<'req, N> {
    type StreamInner = NeverStream;

    fn prepare(
        self,
        mode: Preparation,
        _gzip: Option<&mut Gzip>,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Prepared<Self::StreamInner> {
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
    }

    fn response_view(&self) -> Option<ResponseView> {
        Some(ResponseView {
            status: self.status(),
            headers: self.wire_headers(),
            body: Shared::from_static(self.body_ref()),
        })
    }
}

impl<'req> Shape<'req> for Chunked {
    type StreamInner = NeverStream;

    fn prepare(
        self,
        mode: Preparation,
        _gzip: Option<&mut Gzip>,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Prepared<Self::StreamInner> {
        if mode == Preparation::Cache {
            return Prepared::Egress(Egress::Failed);
        }
        let egress = if let Some(written) = self.write_into_slice(out, date) {
            Egress::Inline { written }
        } else {
            match self.write_head_split(out, date) {
                Some((head, body)) => Egress::Shared { head, body },
                None => Egress::Failed,
            }
        };
        Prepared::Egress(egress)
    }
}

impl<'req, S> Shape<'req> for Stream<S>
where
    S: 'static,
{
    type StreamInner = S;

    fn prepare(
        self,
        mode: Preparation,
        _gzip: Option<&mut Gzip>,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Prepared<Self::StreamInner> {
        if mode == Preparation::Cache {
            return Prepared::Egress(Egress::Failed);
        }
        Prepared::Egress(match self.write_head_stream(out, date) {
            Some((head, stream)) => Egress::Stream { head, stream },
            None => Egress::Failed,
        })
    }
}

impl<'req, const N: usize> Shape<'req> for Serve<'req, N> {
    type StreamInner = NeverStream;

    fn prepare(
        self,
        mode: Preparation,
        gzip: Option<&mut Gzip>,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Prepared<Self::StreamInner> {
        match self {
            Self::Fixed(response) => response.prepare(mode, gzip, out, date),
            Self::Mono(response) => response.prepare(mode, gzip, out, date),
            Self::Chunked(response) => response.prepare(mode, gzip, out, date),
        }
    }

    fn response_view(&self) -> Option<ResponseView> {
        match self {
            Self::Fixed(response) => response.response_view(),
            Self::Mono(response) => response.response_view(),
            Self::Chunked(_) => None,
        }
    }
}

fn fixed_egress<'req, const N: usize>(
    response: FixedResponse<'req, N>,
    out: &mut [u8],
    date: &[u8; 29],
) -> Egress<NeverStream> {
    if let Some(written) = response.write_into_slice(out, date) {
        return Egress::Inline { written };
    }
    match response.write_head_split(out, date) {
        Some((head, body)) => Egress::Shared { head, body },
        None => Egress::Failed,
    }
}
