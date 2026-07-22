use http::StatusCode;
use o3::buffer::{Pooled, Shared};

use crate::http::compress::Gzip;

use super::{
    Chunked, EncodedBody, EncodedResponseInner, FixedResponseInner, MonoResponseInner, NeverStream,
    ServeInner, StaticResponseInner, Stream,
};

pub enum Egress<S> {
    Inline { written: usize },
    Static { head: usize, body: &'static [u8] },
    Shared { head: usize, body: Shared },
    Pooled { head: usize, body: Pooled },
    Stream { head: usize, stream: S },
    Failed,
}

pub enum Compression<T, S> {
    Plain(T),
    Compressed(Egress<S>),
}

pub enum CacheTemplate {
    Inline {
        bytes: Vec<u8>,
        date_offset: usize,
    },
    Static {
        head: Vec<u8>,
        date_offset: usize,
        body: &'static [u8],
    },
}

pub struct ResponseView {
    pub status: StatusCode,
    pub headers: Shared,
    pub body: Shared,
}

pub trait Shape<'req>: Sized {
    type StreamInner: 'static;

    fn egress(self, out: &mut [u8], date: &[u8; 29]) -> Egress<Self::StreamInner>;

    fn compress(
        self,
        _gzip: &mut Gzip,
        _out: &mut [u8],
        _date: &[u8; 29],
    ) -> Compression<Self, Self::StreamInner> {
        Compression::Plain(self)
    }

    fn cache_template(&self) -> Option<CacheTemplate> {
        None
    }

    fn response_view(&self) -> Option<ResponseView> {
        None
    }
}

impl<'req, const N: usize> Shape<'req> for FixedResponseInner<'req, N> {
    type StreamInner = NeverStream;

    fn egress(self, out: &mut [u8], date: &[u8; 29]) -> Egress<Self::StreamInner> {
        if let Some(written) = self.write_into_slice(out, date) {
            return Egress::Inline { written };
        }
        match self.write_head_split(out, date) {
            Some((head, body)) => Egress::Shared { head, body },
            None => Egress::Failed,
        }
    }

    fn compress(
        self,
        gzip: &mut Gzip,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Compression<Self, Self::StreamInner> {
        if self.has_content_encoding() || self.body_ref().is_empty() {
            return Compression::Plain(self);
        }
        let Some(body) = gzip.encode(self.body_ref()) else {
            return Compression::Plain(self);
        };
        let body_len = body.len();
        match self.write_gzip_head(out, date, body_len) {
            Some(head) => Compression::Compressed(Egress::Pooled { head, body }),
            None => Compression::Compressed(Egress::Failed),
        }
    }

    fn cache_template(&self) -> Option<CacheTemplate> {
        let (bytes, date_offset) = self.preserialize();
        Some(CacheTemplate::Inline { bytes, date_offset })
    }

    fn response_view(&self) -> Option<ResponseView> {
        Some(ResponseView {
            status: self.status(),
            headers: self.wire_headers(),
            body: self.body.clone(),
        })
    }
}

impl<'req, B, const N: usize> Shape<'req> for EncodedResponseInner<'req, B, N>
where
    B: EncodedBody,
{
    type StreamInner = NeverStream;

    fn egress(self, out: &mut [u8], date: &[u8; 29]) -> Egress<Self::StreamInner> {
        if let Some(written) = self.write_into_slice(out, date) {
            return Egress::Inline { written };
        }
        match self.write_head_split(out, date) {
            Some((head, body)) => Egress::Shared { head, body },
            None => Egress::Failed,
        }
    }

    fn cache_template(&self) -> Option<CacheTemplate> {
        let (bytes, date_offset) = self.preserialize();
        Some(CacheTemplate::Inline { bytes, date_offset })
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

    fn egress(self, out: &mut [u8], date: &[u8; 29]) -> Egress<Self::StreamInner> {
        if let Some(written) = self.write_into_slice(out, date) {
            return Egress::Inline { written };
        }
        match self.write_head_split(out, date) {
            Some((head, body)) => Egress::Shared { head, body },
            None => Egress::Failed,
        }
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

    fn egress(self, out: &mut [u8], date: &[u8; 29]) -> Egress<Self::StreamInner> {
        match self.write_head_only(out, date) {
            Some((head, body)) => Egress::Static { head, body },
            None => Egress::Failed,
        }
    }

    fn cache_template(&self) -> Option<CacheTemplate> {
        let (head, date_offset, body) = self.preserialize_static();
        Some(CacheTemplate::Static {
            head,
            date_offset,
            body,
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

    fn egress(self, out: &mut [u8], date: &[u8; 29]) -> Egress<Self::StreamInner> {
        if let Some(written) = self.write_into_slice(out, date) {
            return Egress::Inline { written };
        }
        match self.write_head_split(out, date) {
            Some((head, body)) => Egress::Shared { head, body },
            None => Egress::Failed,
        }
    }
}

impl<'req, S> Shape<'req> for Stream<S>
where
    S: 'static,
{
    type StreamInner = S;

    fn egress(self, out: &mut [u8], date: &[u8; 29]) -> Egress<Self::StreamInner> {
        match self.write_head_stream(out, date) {
            Some((head, stream)) => Egress::Stream { head, stream },
            None => Egress::Failed,
        }
    }
}

impl<'req, const N: usize> Shape<'req> for ServeInner<'req, N> {
    type StreamInner = NeverStream;

    fn egress(self, out: &mut [u8], date: &[u8; 29]) -> Egress<Self::StreamInner> {
        match self {
            Self::Fixed(response) => response.egress(out, date),
            Self::Mono(response) => response.egress(out, date),
            Self::Chunked(response) => response.egress(out, date),
        }
    }

    fn compress(
        self,
        gzip: &mut Gzip,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Compression<Self, Self::StreamInner> {
        match self {
            Self::Fixed(response) => match response.compress(gzip, out, date) {
                Compression::Plain(response) => Compression::Plain(Self::Fixed(response)),
                Compression::Compressed(egress) => Compression::Compressed(egress),
            },
            response => Compression::Plain(response),
        }
    }

    fn cache_template(&self) -> Option<CacheTemplate> {
        match self {
            Self::Fixed(response) => response.cache_template(),
            Self::Mono(_) | Self::Chunked(_) => None,
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
