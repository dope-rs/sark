use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Range;
use std::str::FromStr;

use http::{HeaderMap, Method, Uri, Version};
use o3::buffer::{Owned, Shared};

#[derive(Clone)]
enum LocalFrameOwnerRef<'req> {
    Shared(Shared),
    Borrowed(&'req [u8]),
}

pub struct LocalFrameBytesRef<'req> {
    owner: LocalFrameOwnerRef<'req>,
    range: Range<usize>,
    _marker: PhantomData<&'req [u8]>,
}

pub type LocalFrameBytes = LocalFrameBytesRef<'static>;

impl<'req> LocalFrameBytesRef<'req> {
    pub fn len(&self) -> usize {
        self.range.end.saturating_sub(self.range.start)
    }

    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    pub fn from_shared(owner: Shared) -> Self {
        let len = owner.len();
        Self::from_shared_range(owner, 0..len)
    }

    pub fn from_shared_range(owner: Shared, range: Range<usize>) -> Self {
        let len = owner.len();
        assert!(range.start <= range.end, "invalid local frame range");
        assert!(range.end <= len, "local frame range exceeds owner length");
        Self {
            owner: LocalFrameOwnerRef::Shared(owner),
            range,
            _marker: PhantomData,
        }
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn assume_static(self) -> LocalFrameBytes {
        // SAFETY: caller guarantees the backing storage outlives every use of the returned frame.
        unsafe { std::mem::transmute::<LocalFrameBytesRef<'req>, LocalFrameBytes>(self) }
    }

    pub fn from_slice(slice: &'req [u8]) -> Self {
        Self {
            owner: LocalFrameOwnerRef::Borrowed(slice),
            range: 0..slice.len(),
            _marker: PhantomData,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match &self.owner {
            LocalFrameOwnerRef::Shared(bytes) => &bytes[self.range.clone()],
            LocalFrameOwnerRef::Borrowed(slice) => &slice[self.range.clone()],
        }
    }

    pub fn slice(mut self, range: Range<usize>) -> Self {
        let new_start = self.range.start + range.start;
        let new_end = self.range.start + range.end;
        assert!(new_end <= self.range.end, "slice exceeds frame length");
        self.range = new_start..new_end;
        self
    }

    pub fn into_bytes(self) -> Shared {
        match self.owner {
            LocalFrameOwnerRef::Shared(bytes) => bytes.slice(self.range),
            LocalFrameOwnerRef::Borrowed(slice) => Shared::copy_from_slice(&slice[self.range]),
        }
    }
}

impl<'req> Clone for LocalFrameBytesRef<'req> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            range: self.range.clone(),
            _marker: PhantomData,
        }
    }
}

impl<'req> AsRef<[u8]> for LocalFrameBytesRef<'req> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

const INLINE_PATH_PARAM_CAP: usize = 2;

type PathParam = (Box<str>, Range<usize>);

#[derive(Clone)]
enum PathParamStorage {
    Inline {
        items: [Option<PathParam>; INLINE_PATH_PARAM_CAP],
        len: u8,
    },
    Heap(Vec<PathParam>),
}

#[derive(Clone)]
pub struct PathParamRanges(PathParamStorage);

impl Default for PathParamRanges {
    fn default() -> Self {
        Self::new()
    }
}

impl PathParamRanges {
    pub fn new() -> Self {
        Self(PathParamStorage::Inline {
            items: [const { None }; INLINE_PATH_PARAM_CAP],
            len: 0,
        })
    }

    pub fn with_capacity(cap: usize) -> Self {
        if cap > INLINE_PATH_PARAM_CAP {
            Self(PathParamStorage::Heap(Vec::with_capacity(cap)))
        } else {
            Self::new()
        }
    }

    pub fn push(&mut self, key: Box<str>, range: Range<usize>) {
        match &mut self.0 {
            PathParamStorage::Heap(heap) => heap.push((key, range)),
            PathParamStorage::Inline { items, len }
                if usize::from(*len) < INLINE_PATH_PARAM_CAP =>
            {
                items[usize::from(*len)] = Some((key, range));
                *len += 1;
            }
            PathParamStorage::Inline { items, len } => {
                let mut heap = Vec::with_capacity(usize::from(*len).saturating_add(1));
                for slot in items.iter_mut().take(usize::from(*len)) {
                    if let Some(item) = slot.take() {
                        heap.push(item);
                    }
                }
                heap.push((key, range));
                self.0 = PathParamStorage::Heap(heap);
            }
        }
    }

    pub fn find_last(&self, key: &str) -> Option<&Range<usize>> {
        match &self.0 {
            PathParamStorage::Heap(heap) => heap
                .iter()
                .rev()
                .find(|(k, _)| k.as_ref() == key)
                .map(|(_, r)| r),
            PathParamStorage::Inline { items, len } => items
                .iter()
                .take(usize::from(*len))
                .rev()
                .find_map(|slot| slot.as_ref().filter(|(k, _)| k.as_ref() == key))
                .map(|(_, r)| r),
        }
    }
}

pub struct Request {
    method: Method,
    uri: Uri,
    version: Version,
    headers: HeaderMap,
    body: Owned,
    path_params: Vec<(Box<str>, Shared)>,
    query_cache: RefCell<QueryCache>,
}

#[derive(Clone, Debug)]
enum QueryCache {
    Unparsed,
    NoQuery,
    Invalid,
    Parsed(HashMap<String, String>),
}

pub struct PathParamsIter<'a> {
    req: &'a Request,
    idx: usize,
}

impl<'a> Iterator for PathParamsIter<'a> {
    type Item = (&'a str, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.req.path_params.get(self.idx)?;
        self.idx += 1;
        Some((key.as_ref(), value.as_ref()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.req.path_params.len().saturating_sub(self.idx);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for PathParamsIter<'_> {}

impl Request {
    pub fn new(method: Method, uri: Uri) -> Self {
        Self {
            method,
            uri,
            version: Version::HTTP_11,
            headers: HeaderMap::new(),
            body: Owned::new(),
            path_params: Vec::new(),
            query_cache: RefCell::new(QueryCache::Unparsed),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Self {
        Self::new(Method::GET, Uri::from_static("/"))
    }

    pub fn method(&self) -> &Method {
        &self.method
    }
    pub fn uri(&self) -> &Uri {
        &self.uri
    }
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    pub fn body(&self) -> &Owned {
        &self.body
    }

    pub fn body_mut(&mut self) -> &mut Owned {
        &mut self.body
    }

    pub fn set_body(&mut self, body: impl Into<Owned>) {
        self.body = body.into();
    }

    pub fn clear_body(&mut self) {
        self.body.clear();
    }

    pub fn set_body_str(&mut self, body: &str) -> &mut Self {
        self.body = Owned::from(body.as_bytes());
        self
    }

    pub fn body_str(&self) -> Option<&str> {
        std::str::from_utf8(self.body.as_ref()).ok()
    }

    pub fn path_param<T: AsRef<str>>(&self, key: T) -> Option<&str> {
        self.path_param_bytes(key)
            .and_then(|v| std::str::from_utf8(v).ok())
    }

    pub fn path_param_bytes<T: AsRef<str>>(&self, key: T) -> Option<&[u8]> {
        let k = key.as_ref();
        self.path_params
            .iter()
            .rev()
            .find(|(key, _)| key.as_ref() == k)
            .map(|(_, v)| v.as_ref())
    }

    pub fn path_params(&self) -> PathParamsIter<'_> {
        PathParamsIter { req: self, idx: 0 }
    }

    pub fn path_params_len(&self) -> usize {
        self.path_params.len()
    }

    pub fn set_path_params(&mut self, params: Vec<(Box<str>, Shared)>) -> &mut Self {
        self.path_params = params;
        self
    }

    pub fn insert_path_param(
        &mut self,
        key: impl AsRef<str>,
        value: impl Into<Shared>,
    ) -> &mut Self {
        let k = key.as_ref();
        let v = value.into();
        for (key, value) in self.path_params.iter_mut().rev() {
            if key.as_ref() == k {
                *value = v;
                return self;
            }
        }

        self.path_params.push((Box::<str>::from(k), v));
        self
    }

    fn decode_query_component(input: &[u8]) -> Option<String> {
        if !input.contains(&b'%') && !input.contains(&b'+') {
            return std::str::from_utf8(input).ok().map(|s| s.to_string());
        }

        let mut out = Vec::with_capacity(input.len());
        let mut i = 0usize;
        while i < input.len() {
            match input[i] {
                b'+' => {
                    out.push(b' ');
                    i += 1;
                }
                b'%' => {
                    if i + 2 >= input.len() {
                        return None;
                    }
                    let hi = Self::from_hex(input[i + 1])?;
                    let lo = Self::from_hex(input[i + 2])?;
                    out.push((hi << 4) | lo);
                    i += 3;
                }
                b => {
                    out.push(b);
                    i += 1;
                }
            }
        }

        String::from_utf8(out).ok()
    }

    fn from_hex(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }

    fn query_kv_iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.uri
            .query()
            .unwrap_or("")
            .as_bytes()
            .split(|b| *b == b'&')
            .filter(|kv| !kv.is_empty())
            .map(|kv| {
                let mut it = kv.splitn(2, |b| *b == b'=');
                let k = it.next().unwrap_or(&[]);
                let v = it.next().unwrap_or(&[]);
                (k, v)
            })
    }

    pub fn query<T: AsRef<str>>(&self, key: T) -> Option<String> {
        let key = key.as_ref().as_bytes();
        for (k, v) in self.query_kv_iter() {
            if k == key {
                return Self::decode_query_component(v);
            }
            if (k.contains(&b'%') || k.contains(&b'+'))
                && Self::decode_query_component(k).is_some_and(|decoded| decoded.as_bytes() == key)
            {
                return Self::decode_query_component(v);
            }
        }
        None
    }

    pub fn query_bytes<T: AsRef<[u8]>>(&self, key: T) -> Option<&[u8]> {
        let key = key.as_ref();
        self.query_kv_iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v)
    }

    pub fn with_uri(&mut self, uri: Uri) -> &mut Self {
        self.uri = uri;
        *self.query_cache.get_mut() = QueryCache::Unparsed;
        self
    }

    pub fn with_query<T: serde::Serialize>(
        &mut self,
        query: &T,
    ) -> Result<&mut Self, serde_urlencoded::ser::Error> {
        let query_string = serde_urlencoded::to_string(query)?;
        let current_uri = self.uri.clone();

        let mut parts = current_uri.into_parts();
        parts.path_and_query = Some(match parts.path_and_query {
            Some(path_and_query) => {
                let path = path_and_query.path();
                let new_path_and_query = format!("{}?{}", path, query_string);
                http::uri::PathAndQuery::from_str(&new_path_and_query).map_err(|e| {
                    <serde_urlencoded::ser::Error as serde::ser::Error>::custom(format!(
                        "invalid path and query: {e}"
                    ))
                })?
            }
            None => {
                let new_path_and_query = format!("/?{query_string}");
                http::uri::PathAndQuery::from_str(&new_path_and_query).map_err(|e| {
                    <serde_urlencoded::ser::Error as serde::ser::Error>::custom(format!(
                        "invalid path and query: {e}"
                    ))
                })?
            }
        });

        let new_uri = Uri::from_parts(parts).map_err(|e| {
            <serde_urlencoded::ser::Error as serde::ser::Error>::custom(format!(
                "invalid uri parts: {e}"
            ))
        })?;

        self.uri = new_uri;
        *self.query_cache.get_mut() = QueryCache::Unparsed;
        Ok(self)
    }

    pub fn query_params_ref(&self) -> Option<std::cell::Ref<'_, HashMap<String, String>>> {
        {
            let mut cache = self.query_cache.borrow_mut();
            if matches!(*cache, QueryCache::Unparsed) {
                *cache = match self.uri.query() {
                    None => QueryCache::NoQuery,
                    Some(q) => match serde_urlencoded::from_str::<HashMap<String, String>>(q) {
                        Ok(m) => QueryCache::Parsed(m),
                        Err(_) => QueryCache::Invalid,
                    },
                };
            }
        }

        std::cell::Ref::filter_map(self.query_cache.borrow(), |c| match c {
            QueryCache::Parsed(m) => Some(m),
            QueryCache::Invalid => None,
            QueryCache::NoQuery => None,
            QueryCache::Unparsed => None,
        })
        .ok()
    }
}

impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Request")
            .field("method", &self.method)
            .field("uri", &self.uri)
            .field("version", &self.version)
            .field("headers", self.headers())
            .field("body_len", &self.body.len())
            .field("path_params", &self.path_params.len())
            .finish()
    }
}

impl Clone for Request {
    fn clone(&self) -> Self {
        let query_cache = self.query_cache.borrow().clone();
        Self {
            method: self.method.clone(),
            uri: self.uri.clone(),
            version: self.version,
            headers: self.headers.clone(),
            body: self.body.clone(),
            path_params: self.path_params.clone(),
            query_cache: RefCell::new(query_cache),
        }
    }
}

#[cfg(test)]
mod tests;
