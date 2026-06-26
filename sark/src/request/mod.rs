use std::marker::PhantomData;
use std::ops::Range;
use std::slice;

use http::{Method, Version};
use o3::buffer::Shared;
use sark_core::http::{LocalFrameBytes, LocalFrameBytesRef, PathParamRanges};

mod chain;
mod chain_ref;
mod path;

pub(crate) use chain::SplitFrameChain;
pub(crate) use chain_ref::SplitFrameChainRef;
pub(crate) use path::{BodyChainIter, BodyChunks, PathView};

pub struct UriView<'a> {
    head: &'a SplitFrameChain,
    start: usize,
    path_end: usize,
    end: usize,
}

impl<'a> UriView<'a> {
    pub fn raw(&self) -> PathView<'a> {
        self.head.path_view(self.start..self.end)
    }

    pub fn path(&self) -> PathView<'a> {
        self.head.path_view(self.start..self.path_end)
    }

    pub fn query(&self) -> Option<PathView<'a>> {
        if self.path_end >= self.end {
            return None;
        }
        Some(self.head.path_view((self.path_end + 1)..self.end))
    }
}

pub struct Body {
    body: SplitFrameChain,
}

impl Body {
    pub fn len(&self) -> usize {
        self.body.len()
    }

    pub fn is_empty(&self) -> bool {
        self.body.len() == 0
    }

    pub fn into_local(self) -> LocalFrameBytes {
        let range = 0..self.body.len();
        if let Some(local) = self.body.local_direct(range) {
            return local;
        }
        self.body.compact().clone()
    }

    pub fn into_bytes(self) -> Shared {
        self.into_local().into_bytes()
    }
}

#[derive(Clone, Copy)]
pub struct BodyLen {
    len: usize,
}

impl BodyLen {
    pub const fn from_declared(len: usize) -> Self {
        Self { len }
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

pub(super) struct ChunkerCore {
    pub(super) skip: usize,
    pub(super) remaining: usize,
}

impl ChunkerCore {
    pub(super) fn step<'a>(&mut self, bytes: &'a [u8]) -> Option<&'a [u8]> {
        if self.skip >= bytes.len() {
            self.skip -= bytes.len();
            return None;
        }
        let start = self.skip;
        let take = (bytes.len() - start).min(self.remaining);
        self.skip = 0;
        self.remaining -= take;
        Some(&bytes[start..start + take])
    }
}

struct UriPathOps;

impl UriPathOps {
    fn end_from(frame: &[u8], uri_range: &Range<usize>) -> usize {
        match frame.get(uri_range.clone()) {
            Some(seg) => seg
                .iter()
                .position(|b| *b == b'?')
                .map(|off| uri_range.start + off)
                .unwrap_or(uri_range.end),
            None => uri_range.end,
        }
    }
}

trait UriPath {
    fn uri_range_start(&self) -> usize;
    fn uri_range_end(&self) -> usize;
    fn uri_path_end_value(&self) -> usize;

    fn path_len(&self) -> usize {
        self.uri_path_end_value()
            .saturating_sub(self.uri_range_start())
    }

    fn path_abs(&self, range: &Range<usize>) -> Option<Range<usize>> {
        if range.start > range.end || range.end > self.path_len() {
            return None;
        }
        Some((self.uri_range_start() + range.start)..(self.uri_range_start() + range.end))
    }

    fn uri_query_range(&self) -> Option<Range<usize>> {
        if self.uri_path_end_value() >= self.uri_range_end() {
            return None;
        }
        Some((self.uri_path_end_value() + 1)..self.uri_range_end())
    }
}

pub struct Request<H = ()> {
    method: Method,
    head: SplitFrameChain,
    body: SplitFrameChain,
    uri_range: Range<usize>,
    uri_path_end: usize,
    version: Version,
    path_param_ranges: Option<PathParamRanges>,
    declared_body_len: usize,
    _headers: PhantomData<fn() -> H>,
}

impl<H> Request<H> {
    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn uri(&self) -> UriView<'_> {
        UriView {
            head: &self.head,
            start: self.uri_range.start,
            path_end: self.uri_path_end,
            end: self.uri_range.end,
        }
    }

    pub fn path_view(&self) -> PathView<'_> {
        let path_range = self.uri_range.start..self.uri_path_end;
        self.head.path_view(path_range)
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn head_end(&self) -> usize {
        self.head.len()
    }

    pub fn query_range(&self) -> Option<Range<usize>> {
        UriPath::uri_query_range(self)
    }

    pub fn path_param_view<T: AsRef<str>>(&self, key: T) -> Option<PathView<'_>> {
        self.path_param_ranges
            .as_ref()?
            .find_last(key.as_ref())
            .and_then(|range| self.path_at(range))
    }

    pub fn path_param_u64<T: AsRef<str>>(&self, key: T) -> Option<u64> {
        self.path_param_view(key).and_then(PathView::parse_u64)
    }

    pub fn path_slice(&self, range: Range<usize>) -> Option<&[u8]> {
        self.path_at(&range).and_then(PathView::as_slice)
    }

    pub fn path_local(&self, range: Range<usize>) -> Option<LocalFrameBytes> {
        let abs = self.path_abs(&range)?;
        if let Some(local) = self.head.local_direct(abs.clone()) {
            return Some(local);
        }
        Some(self.head.compact().clone().slice(abs))
    }

    pub fn path_u64(&self, range: Range<usize>) -> Option<u64> {
        self.path_at(&range).and_then(PathView::parse_u64)
    }

    pub fn at(&self, range: &Range<usize>) -> Option<&[u8]> {
        self.head.bytes_range(range.clone())
    }

    pub fn local_at(&self, range: Range<usize>) -> Option<LocalFrameBytes> {
        if let Some(local) = self.head.local_direct(range.clone()) {
            return Some(local);
        }
        if range.start > range.end || range.end > self.head.len() {
            return None;
        }
        Some(self.head.compact().clone().slice(range))
    }

    pub fn into_body(self) -> Body {
        let Self { body, .. } = self;
        Body { body }
    }

    pub fn body_chunks(&self) -> BodyChunks<'_> {
        BodyChainIter::new(&self.body, 0..self.body.len())
    }

    pub fn declared_body_len(&self) -> usize {
        self.declared_body_len
    }

    pub fn set_declared_body_len(&mut self, len: usize) {
        self.declared_body_len = len;
    }

    fn path_at(&self, range: &Range<usize>) -> Option<PathView<'_>> {
        let abs = self.path_abs(range)?;
        Some(self.head.path_view(abs))
    }
}

impl<H> UriPath for Request<H> {
    fn uri_range_start(&self) -> usize {
        self.uri_range.start
    }
    fn uri_range_end(&self) -> usize {
        self.uri_range.end
    }
    fn uri_path_end_value(&self) -> usize {
        self.uri_path_end
    }
}

impl Request<()> {
    pub fn new(method: Method, uri_raw: impl AsRef<[u8]>) -> Self {
        let uri_raw = uri_raw.as_ref();
        let uri_raw = if uri_raw.is_empty() { b"/" } else { uri_raw };
        debug_assert!(
            uri_raw.first() == Some(&b'/') || uri_raw == b"*",
            "request new uri must be origin-form path or asterisk form",
        );
        let frame = Shared::copy_from_slice(uri_raw);
        let len = frame.len();
        let uri_range = 0..len;
        let uri_path_end = UriPathOps::end_from(frame.as_ref(), &uri_range);
        let frame = LocalFrameBytes::from_shared(frame);
        let mut head = SplitFrameChain::new();
        head.push(frame.slice(0..len));
        let body = SplitFrameChain::new();
        Self::from_ingress(
            method,
            Version::HTTP_11,
            head,
            uri_range,
            uri_path_end,
            body,
        )
    }

    pub fn from_head_and_body(
        method: Method,
        uri_range: Range<usize>,
        head_bytes: &[u8],
        body_bytes: &[u8],
    ) -> Self {
        let uri_path_end = UriPathOps::end_from(head_bytes, &uri_range);
        let head_frame = LocalFrameBytes::from_shared(Shared::copy_from_slice(head_bytes));
        let mut head = SplitFrameChain::new();
        head.push(head_frame);
        let mut body = SplitFrameChain::new();
        if !body_bytes.is_empty() {
            let body_frame = LocalFrameBytes::from_shared(Shared::copy_from_slice(body_bytes));
            body.push(body_frame);
        }
        Self::from_ingress(
            method,
            Version::HTTP_11,
            head,
            uri_range,
            uri_path_end,
            body,
        )
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn from_borrowed_static(
        method: Method,
        uri_range: Range<usize>,
        head_bytes: &[u8],
        body_bytes: &[u8],
    ) -> Self {
        let head_len = head_bytes.len();
        let uri_path_end = UriPathOps::end_from(head_bytes, &uri_range);
        // SAFETY: caller-bound — conn ingress_buf outlives this Request.
        let head_frame = unsafe { LocalFrameBytesRef::from_slice(head_bytes).assume_static() };
        let mut head = SplitFrameChain::new();
        head.push(head_frame.slice(0..head_len));
        let mut body = SplitFrameChain::new();
        if !body_bytes.is_empty() {
            let body_len = body_bytes.len();
            // SAFETY: same as head_frame — conn ingress_buf outlives this Request.
            let body_frame = unsafe { LocalFrameBytesRef::from_slice(body_bytes).assume_static() };
            body.push(body_frame.slice(0..body_len));
        }
        Self::from_ingress(
            method,
            Version::HTTP_11,
            head,
            uri_range,
            uri_path_end,
            body,
        )
    }

    fn assert_uri_path_end(uri_range: &Range<usize>, uri_path_end: usize) {
        debug_assert!(
            uri_range.start <= uri_path_end && uri_path_end <= uri_range.end,
            "uri_path_end must be inside uri_range"
        );
    }

    pub(crate) fn from_ingress(
        method: Method,
        version: Version,
        head: SplitFrameChain,
        uri_range: Range<usize>,
        uri_path_end: usize,
        body: SplitFrameChain,
    ) -> Self {
        Self::assert_uri_path_end(&uri_range, uri_path_end);
        let declared_body_len = body.len();
        Self {
            method,
            version,
            uri_range,
            uri_path_end,
            head,
            body,
            path_param_ranges: None,
            declared_body_len,
            _headers: PhantomData,
        }
    }

    pub fn with_headers_ready<Hdr>(self) -> Request<Hdr> {
        let Self {
            method,
            head,
            body,
            uri_range,
            uri_path_end,
            version,
            path_param_ranges,
            declared_body_len,
            ..
        } = self;
        Request {
            method,
            head,
            body,
            uri_range,
            uri_path_end,
            version,
            path_param_ranges,
            declared_body_len,
            _headers: PhantomData,
        }
    }
}

pub struct Ref<'req, H = ()> {
    method: Method,
    head: SplitFrameChainRef<'req>,
    body: SplitFrameChainRef<'req>,
    uri_range: Range<usize>,
    uri_path_end: usize,
    version: Version,
    path_param_ranges: Option<PathParamRanges>,
    declared_body_len: usize,
    _headers: PhantomData<fn() -> H>,
}

impl<'req, H> Ref<'req, H> {
    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn head_end(&self) -> usize {
        self.head.len()
    }

    pub fn uri_range(&self) -> Range<usize> {
        self.uri_range.clone()
    }

    pub fn uri_path_end(&self) -> usize {
        self.uri_path_end
    }

    pub fn path_view(&self) -> PathView<'_> {
        self.head.path_view(self.uri_range.start..self.uri_path_end)
    }

    pub fn query_range(&self) -> Option<Range<usize>> {
        UriPath::uri_query_range(self)
    }

    pub fn path_at(&self, range: &Range<usize>) -> Option<PathView<'_>> {
        let abs = self.path_abs(range)?;
        Some(self.head.path_view(abs))
    }

    pub fn path_local(&self, range: Range<usize>) -> Option<LocalFrameBytesRef<'req>> {
        let abs = self.path_abs(&range)?;
        self.head.local_direct(abs)
    }

    pub fn at(&self, range: &Range<usize>) -> Option<&[u8]> {
        self.head.bytes_range(range.clone())
    }

    pub fn local_at(&self, range: Range<usize>) -> Option<LocalFrameBytesRef<'req>> {
        self.head.local_direct(range)
    }

    pub fn path_param_view<T: AsRef<str>>(&self, key: T) -> Option<PathView<'_>> {
        self.path_param_ranges
            .as_ref()?
            .find_last(key.as_ref())
            .and_then(|range| self.path_at(range))
    }

    pub fn path_param_u64<T: AsRef<str>>(&self, key: T) -> Option<u64> {
        self.path_param_view(key).and_then(PathView::parse_u64)
    }

    pub fn body_chunks(&self) -> BodyChunksRef<'_, 'req> {
        BodyChunksRef {
            frames: self.body.iter_frames(),
            core: ChunkerCore {
                skip: 0,
                remaining: self.body.len(),
            },
        }
    }

    pub fn body_len(&self) -> usize {
        self.body.len()
    }

    pub fn declared_body_len(&self) -> usize {
        self.declared_body_len
    }

    pub fn set_declared_body_len(&mut self, len: usize) {
        self.declared_body_len = len;
    }

    pub fn body_owned(&self) -> Body {
        let mut buf: Vec<u8> = Vec::with_capacity(self.body.len());
        for chunk in self.body_chunks() {
            buf.extend_from_slice(chunk);
        }
        let mut body = SplitFrameChain::new();
        if !buf.is_empty() {
            body.push(LocalFrameBytes::from_shared(Shared::from(buf)));
        }
        Body { body }
    }
}

impl<'req, H> UriPath for Ref<'req, H> {
    fn uri_range_start(&self) -> usize {
        self.uri_range.start
    }
    fn uri_range_end(&self) -> usize {
        self.uri_range.end
    }
    fn uri_path_end_value(&self) -> usize {
        self.uri_path_end
    }
}

impl<'req> Ref<'req, ()> {
    pub fn from_slice(
        method: Method,
        uri_range: Range<usize>,
        head: &'req [u8],
        body: &'req [u8],
    ) -> Self {
        let uri_path_end = UriPathOps::end_from(head, &uri_range);
        debug_assert!(
            uri_range.start <= uri_path_end && uri_path_end <= uri_range.end,
            "uri_path_end must be inside uri_range"
        );
        let mut head_chain = SplitFrameChainRef::new();
        if !head.is_empty() {
            head_chain.push(LocalFrameBytesRef::from_slice(head));
        }
        let mut body_chain = SplitFrameChainRef::new();
        if !body.is_empty() {
            body_chain.push(LocalFrameBytesRef::from_slice(body));
        }
        let declared_body_len = body.len();
        Ref {
            method,
            version: Version::HTTP_11,
            head: head_chain,
            body: body_chain,
            uri_range,
            uri_path_end,
            path_param_ranges: None,
            declared_body_len,
            _headers: PhantomData,
        }
    }

    pub fn with_headers_ready<Hdr>(self) -> Ref<'req, Hdr> {
        let Ref {
            method,
            head,
            body,
            uri_range,
            uri_path_end,
            version,
            path_param_ranges,
            declared_body_len,
            ..
        } = self;
        Ref {
            method,
            head,
            body,
            uri_range,
            uri_path_end,
            version,
            path_param_ranges,
            declared_body_len,
            _headers: PhantomData,
        }
    }
}

pub struct BodyChunksRef<'a, 'req> {
    frames: slice::Iter<'a, LocalFrameBytesRef<'req>>,
    core: ChunkerCore,
}

impl<'a, 'req> Iterator for BodyChunksRef<'a, 'req> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        while self.core.remaining != 0 {
            let bytes = self.frames.next()?.as_bytes();
            if let Some(out) = self.core.step(bytes) {
                return Some(out);
            }
        }
        None
    }
}
