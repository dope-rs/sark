use o3::buffer::{Owned, Shared};

use super::value::{HeaderItemInner, HeaderValueInner, InlineHeaderValue};
use crate::http::request::LocalFrameBytesRef;

const INLINE_HEADERS: usize = 4;
pub(in crate::http::response) const INLINE_HOT_TEXT_PARTS: usize = 10;

pub struct HeadersInner<'req> {
    entries: [HeaderItemInner<'req>; INLINE_HEADERS],
    len: u8,
    wire_len: usize,
}

pub type Headers = HeadersInner<'static>;

impl<'req> Clone for HeadersInner<'req> {
    fn clone(&self) -> Self {
        let len = usize::from(self.len);
        let entries = std::array::from_fn(|idx| {
            if idx < len {
                self.entries[idx].clone()
            } else {
                HeaderItemInner::placeholder()
            }
        });
        Self {
            entries,
            len: self.len,
            wire_len: self.wire_len,
        }
    }
}

impl<'req> std::fmt::Debug for HeadersInner<'req> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Headers")
            .field("len", &self.len)
            .field("wire_len", &self.wire_len)
            .finish()
    }
}

impl<'req> Default for HeadersInner<'req> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'req> HeadersInner<'req> {
    pub fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| HeaderItemInner::placeholder()),
            len: 0,
            wire_len: 0,
        }
    }

    pub fn from_items<const N: usize>(items: [HeaderItemInner<'req>; N]) -> Self {
        const {
            assert!(N <= INLINE_HEADERS, "direct header overflow");
        }
        let mut iter = IntoIterator::into_iter(items);
        let mut wire_len = 0usize;
        let entries = std::array::from_fn(|i| {
            if i < N {
                let item = iter.next().expect("from_fn within N must yield Some");
                wire_len += item.wire_len();
                item
            } else {
                HeaderItemInner::placeholder()
            }
        });
        Self {
            entries,
            len: N as u8,
            wire_len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn len(&self) -> usize {
        usize::from(self.len)
    }

    pub fn wire_len(&self) -> usize {
        self.wire_len
    }

    pub fn has_content_encoding(&self) -> bool {
        self.entries[..usize::from(self.len)]
            .iter()
            .any(|e| e.name.as_str().eq_ignore_ascii_case("content-encoding"))
    }

    pub fn push_static(&mut self, name: &'static str, value: HeaderStaticValueToken) -> &mut Self {
        self.push_value(name, HeaderValueInner::Static(value.as_bytes()))
    }

    pub fn push_shared(&mut self, name: &'static str, value: Shared) -> &mut Self {
        self.push_value(name, HeaderValueInner::Shared(value))
    }

    pub fn push_inline(&mut self, name: &'static str, value: InlineHeaderValue) -> &mut Self {
        self.push_value(name, HeaderValueInner::Inline(value))
    }

    pub fn push_local(&mut self, name: &'static str, value: LocalFrameBytesRef<'req>) -> &mut Self {
        self.push_value(name, HeaderValueInner::Local(value))
    }

    pub fn write_into(&self, out: &mut Owned) {
        for idx in 0..usize::from(self.len) {
            let header = &self.entries[idx];
            out.extend_from_slice(header.name_bytes());
            out.extend_from_slice(b": ");
            out.extend_from_slice(header.value_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }

    pub fn write(&self, out: &mut [u8]) -> usize {
        let mut off = 0usize;
        for idx in 0..usize::from(self.len) {
            let header = &self.entries[idx];
            let name = header.name_bytes();
            let value = header.value_bytes();
            let name_end = off + name.len();
            out[off..name_end].copy_from_slice(name);
            off = name_end;
            out[off..off + 2].copy_from_slice(b": ");
            off += 2;
            let value_end = off + value.len();
            out[off..value_end].copy_from_slice(value);
            off = value_end;
            out[off..off + 2].copy_from_slice(b"\r\n");
            off += 2;
        }
        off
    }

    fn push_value(&mut self, name: &'static str, value: HeaderValueInner<'req>) -> &mut Self {
        assert!(
            usize::from(self.len) < INLINE_HEADERS,
            "direct header overflow: max {}",
            INLINE_HEADERS
        );
        self.wire_len += name.len() + 2 + value.len() + 2;
        self.entries[usize::from(self.len)] = HeaderItemInner {
            name: HeaderNameToken::new(name),
            value,
        };
        self.len += 1;
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeaderNameToken(&'static str);

impl HeaderNameToken {
    pub fn new(name: &'static str) -> Self {
        HeaderAssert::name(name);
        Self(name)
    }

    pub(crate) const fn empty_placeholder() -> Self {
        Self("")
    }

    pub fn as_str(self) -> &'static str {
        self.0
    }

    pub fn as_bytes(self) -> &'static [u8] {
        self.0.as_bytes()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeaderStaticValueToken(&'static str);

impl HeaderStaticValueToken {
    pub fn new(value: &'static str) -> Self {
        HeaderAssert::value(value);
        Self(value)
    }

    pub fn as_str(self) -> &'static str {
        self.0
    }

    pub fn as_bytes(self) -> &'static [u8] {
        self.0.as_bytes()
    }
}

pub(super) struct HeaderAssert;

impl HeaderAssert {
    pub(super) fn name(name: &str) {
        assert!(!name.is_empty(), "direct header name must not be empty");
        assert!(
            !name
                .as_bytes()
                .iter()
                .any(|b| *b == b':' || *b == b'\r' || *b == b'\n'),
            "direct header name must not contain separators"
        );
        assert!(
            !matches!(
                name,
                "date" | "server" | "content-length" | "connection" | "transfer-encoding"
            ),
            "direct header must not override managed headers: {name}"
        );
    }

    pub(super) fn value(value: &str) {
        Self::value_bytes(value.as_bytes());
    }

    pub(super) fn value_bytes(value: &[u8]) {
        assert!(
            !value.iter().any(|b| *b == b'\r' || *b == b'\n'),
            "direct header value must not contain CR/LF"
        );
    }
}
