use o3::buffer::{Borrowed, Bytes, Retained, Shared};

use super::value::{HeaderItemInner, HeaderValueInner, InlineHeaderValue};

pub const DEFAULT_HEADER_CAPACITY: usize = 4;
pub(in crate::http::response) const INLINE_HOT_TEXT_PARTS: usize = 10;

pub struct HeadersInner<'req, const N: usize = DEFAULT_HEADER_CAPACITY> {
    entries: [HeaderItemInner<'req>; N],
    len: u8,
    wire_len: usize,
}

pub type Headers = HeadersInner<'static>;

impl<'req, const N: usize> Clone for HeadersInner<'req, N> {
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

impl<'req, const N: usize> std::fmt::Debug for HeadersInner<'req, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Headers")
            .field("len", &self.len)
            .field("wire_len", &self.wire_len)
            .finish()
    }
}

impl<'req, const N: usize> Default for HeadersInner<'req, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'req, const N: usize> HeadersInner<'req, N> {
    pub fn new() -> Self {
        const {
            assert!(N <= u8::MAX as usize, "direct header count exceeds u8");
        }
        Self {
            entries: std::array::from_fn(|_| HeaderItemInner::placeholder()),
            len: 0,
            wire_len: 0,
        }
    }

    pub fn from_items(items: [HeaderItemInner<'req>; N]) -> Self {
        const {
            assert!(N <= u8::MAX as usize, "direct header count exceeds u8");
        }
        let wire_len = items.iter().map(HeaderItemInner::wire_len).sum();
        Self {
            entries: items,
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

    pub fn push_static(
        &mut self,
        name: HeaderNameToken,
        value: HeaderStaticValueToken,
    ) -> &mut Self {
        self.push_value(name, HeaderValueInner::Static(value.as_bytes()))
    }

    pub fn push_shared(&mut self, name: HeaderNameToken, value: Shared) -> &mut Self {
        self.push_value(name, HeaderValueInner::Shared(value))
    }

    pub fn push_inline(&mut self, name: HeaderNameToken, value: InlineHeaderValue) -> &mut Self {
        self.push_value(name, HeaderValueInner::Inline(value))
    }

    pub fn push_borrowed(
        &mut self,
        name: HeaderNameToken,
        value: Bytes<Borrowed<'req>>,
    ) -> &mut Self {
        self.push_value(name, HeaderValueInner::Borrowed(value))
    }

    pub fn push_retained(&mut self, name: HeaderNameToken, value: Bytes<Retained>) -> &mut Self {
        self.push_value(name, HeaderValueInner::Retained(value))
    }

    pub fn write_into(&self, out: &mut Vec<u8>) {
        self.write_into_buffer(out);
    }

    pub(super) fn write_into_buffer(&self, out: &mut impl super::WireBuffer) {
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

    fn push_value(&mut self, name: HeaderNameToken, value: HeaderValueInner<'req>) -> &mut Self {
        assert!(
            usize::from(self.len) < N,
            "direct header overflow: max {}",
            N
        );
        self.wire_len += name.as_str().len() + 2 + value.len() + 2;
        self.entries[usize::from(self.len)] = HeaderItemInner { name, value };
        self.len += 1;
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeaderNameToken(&'static str);

impl HeaderNameToken {
    pub const fn new(name: &'static str) -> Self {
        HeaderAssert::name(name);
        Self(name)
    }

    pub(crate) const fn empty_placeholder() -> Self {
        Self("")
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }

    pub const fn as_bytes(self) -> &'static [u8] {
        self.0.as_bytes()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeaderStaticValueToken(&'static str);

impl HeaderStaticValueToken {
    pub const fn new(value: &'static str) -> Self {
        HeaderAssert::value(value);
        Self(value)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }

    pub const fn as_bytes(self) -> &'static [u8] {
        self.0.as_bytes()
    }
}

pub(super) struct HeaderAssert;

impl HeaderAssert {
    pub(super) const fn name(name: &str) {
        match sark_protocol::validate_response_header_name(name) {
            Ok(()) => {}
            Err(sark_protocol::ResponseHeaderNameError::Empty) => {
                panic!("direct header name must not be empty")
            }
            Err(sark_protocol::ResponseHeaderNameError::InvalidByte { .. }) => {
                panic!("direct header name contains a non-token byte")
            }
            Err(sark_protocol::ResponseHeaderNameError::Managed) => {
                panic!("direct header must not override a managed header")
            }
        }
    }

    pub(super) const fn value(value: &str) {
        Self::value_bytes(value.as_bytes());
    }

    pub(super) const fn value_bytes(value: &[u8]) {
        assert!(
            sark_protocol::validate_header_value(value).is_ok(),
            "direct header value must not contain CR/LF"
        );
    }
}
