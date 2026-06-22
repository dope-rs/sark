use o3::buffer::{Owned, Shared};

use super::direct::INLINE_HOT_TEXT_PARTS;
use super::{BodyInner, HeadInner, LocalFrameBytesRef};

#[derive(Clone)]
pub enum TextItemInner<'req> {
    Static(&'static [u8]),
    Shared(Shared),
    Local(LocalFrameBytesRef<'req>),
}

pub type TextItem = TextItemInner<'static>;

pub type TextBody = HotTextInner<'static>;

impl<'req> std::fmt::Debug for TextItemInner<'req> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static(bytes) => f
                .debug_struct("TextItem::Static")
                .field("len", &bytes.len())
                .finish(),
            Self::Shared(bytes) => f
                .debug_struct("TextItem::Shared")
                .field("len", &bytes.len())
                .finish(),
            Self::Local(bytes) => f
                .debug_struct("TextItem::Local")
                .field("len", &bytes.len())
                .finish(),
        }
    }
}

impl<'req> TextItemInner<'req> {
    pub(crate) fn placeholder() -> Self {
        Self::Static(&[])
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Static(bytes) => bytes,
            Self::Shared(bytes) => bytes.as_ref(),
            Self::Local(bytes) => bytes.as_bytes(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.as_bytes().len()
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum HotHeadInner<'req> {
    Wire(Shared),
    Direct(HeadInner<'req>),
}

impl<'req> HotHeadInner<'req> {
    pub(super) fn into_bytes(self) -> Shared {
        match self {
            Self::Wire(bytes) => bytes,
            Self::Direct(head) => {
                let mut out = Owned::with_capacity(head.wire_len());
                head.write_into(&mut out);
                out.freeze()
            }
        }
    }
}

pub struct HotTextInner<'req> {
    items: [TextItemInner<'req>; INLINE_HOT_TEXT_PARTS],
    len: u8,
    body_len: usize,
}

impl<'req> Clone for HotTextInner<'req> {
    fn clone(&self) -> Self {
        let len = usize::from(self.len);
        let items = std::array::from_fn(|idx| {
            if idx < len {
                self.items[idx].clone()
            } else {
                TextItemInner::placeholder()
            }
        });
        Self {
            items,
            len: self.len,
            body_len: self.body_len,
        }
    }
}

impl<'req> std::fmt::Debug for HotTextInner<'req> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HotText")
            .field("len", &self.len)
            .field("body_len", &self.body_len)
            .finish()
    }
}

impl<'req> Default for HotTextInner<'req> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'req> HotTextInner<'req> {
    pub fn new() -> Self {
        Self {
            items: std::array::from_fn(|_| TextItemInner::placeholder()),
            len: 0,
            body_len: 0,
        }
    }

    pub fn from_items<const N: usize>(items: [TextItemInner<'req>; N]) -> Self {
        assert!(
            N <= INLINE_HOT_TEXT_PARTS,
            "hot text part overflow: max {}",
            INLINE_HOT_TEXT_PARTS
        );
        let mut iter = IntoIterator::into_iter(items);
        let mut body_len = 0usize;
        let entries = std::array::from_fn(|_| match iter.next() {
            Some(item) => {
                body_len += item.len();
                item
            }
            None => TextItemInner::placeholder(),
        });
        Self {
            items: entries,
            len: N as u8,
            body_len,
        }
    }

    pub fn from_static(bytes: &'static [u8]) -> Self {
        let mut body = Self::new();
        body.push_static(bytes);
        body
    }

    pub fn len(&self) -> usize {
        self.body_len
    }

    pub fn is_empty(&self) -> bool {
        self.body_len == 0
    }

    pub fn push_static(&mut self, bytes: &'static [u8]) -> &mut Self {
        if !bytes.is_empty() {
            self.push_item(TextItemInner::Static(bytes));
        }
        self
    }

    pub fn push_local(&mut self, bytes: LocalFrameBytesRef<'req>) -> &mut Self {
        if !bytes.is_empty() {
            self.push_item(TextItemInner::Local(bytes));
        }
        self
    }

    pub(crate) fn write_to(&self, out: &mut [u8]) -> usize {
        let mut off = 0usize;
        for item in &self.items[..usize::from(self.len)] {
            let bytes = item.as_bytes();
            let end = off + bytes.len();
            out[off..end].copy_from_slice(bytes);
            off = end;
        }
        off
    }

    pub fn into_bytes(self) -> Shared {
        let mut out = Owned::with_capacity(self.body_len);
        for item in &self.items[..usize::from(self.len)] {
            out.extend_from_slice(item.as_bytes());
        }
        out.freeze()
    }

    fn push_item(&mut self, item: TextItemInner<'req>) {
        assert!(
            usize::from(self.len) < INLINE_HOT_TEXT_PARTS,
            "hot text part overflow: max {}",
            INLINE_HOT_TEXT_PARTS
        );
        self.body_len += item.len();
        self.items[usize::from(self.len)] = item;
        self.len += 1;
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum HotBodyInner<'req> {
    Owned(Owned),
    Shared(Shared),
    Local(LocalFrameBytesRef<'req>),
    Text(HotTextInner<'req>),
    StaticSlice(&'static [u8]),
}

impl<'req> HotBodyInner<'req> {
    pub(crate) fn body_len(&self) -> usize {
        match self {
            Self::Owned(body) => body.len(),
            Self::Shared(body) => body.len(),
            Self::Local(body) => body.len(),
            Self::Text(body) => body.len(),
            Self::StaticSlice(body) => body.len(),
        }
    }

    pub(crate) fn write_to(&self, out: &mut [u8]) -> usize {
        match self {
            Self::Owned(body) => {
                let s = body.as_ref();
                out[..s.len()].copy_from_slice(s);
                s.len()
            }
            Self::Shared(body) => {
                let s = body.as_ref();
                out[..s.len()].copy_from_slice(s);
                s.len()
            }
            Self::Local(body) => {
                let s = body.as_bytes();
                out[..s.len()].copy_from_slice(s);
                s.len()
            }
            Self::Text(body) => body.write_to(out),
            Self::StaticSlice(body) => {
                out[..body.len()].copy_from_slice(body);
                body.len()
            }
        }
    }

    pub(crate) fn into_shared(self) -> Shared {
        match self {
            Self::Owned(body) => body.freeze(),
            Self::Shared(body) => body,
            Self::Local(body) => Shared::copy_from_slice(body.as_bytes()),
            Self::Text(body) => body.into_bytes(),
            Self::StaticSlice(body) => Shared::from_static(body),
        }
    }
}

impl<'req> From<BodyInner<'req>> for HotBodyInner<'req> {
    fn from(body: BodyInner<'req>) -> Self {
        match body {
            BodyInner::Owned(body) => Self::Owned(body),
            BodyInner::Shared(body) => Self::Shared(body),
            BodyInner::Local(body) => Self::Local(body),
            BodyInner::StaticSlice(body) => Self::StaticSlice(body),
        }
    }
}

impl From<HotBodyInner<'static>> for BodyInner<'static> {
    fn from(body: HotBodyInner<'static>) -> Self {
        match body {
            HotBodyInner::Owned(body) => Self::Owned(body),
            HotBodyInner::Shared(body) => Self::Shared(body),
            HotBodyInner::Local(body) => Self::Local(body),
            HotBodyInner::Text(body) => Self::Shared(body.into_bytes()),
            HotBodyInner::StaticSlice(body) => Self::StaticSlice(body),
        }
    }
}

impl<'req> std::fmt::Debug for HotBodyInner<'req> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Owned(body) => f
                .debug_struct("HotBody::Owned")
                .field("len", &body.len())
                .finish(),
            Self::Shared(body) => f
                .debug_struct("HotBody::Shared")
                .field("len", &body.len())
                .finish(),
            Self::Local(body) => f
                .debug_struct("HotBody::Local")
                .field("len", &body.len())
                .finish(),
            Self::Text(body) => f
                .debug_struct("HotBody::Text")
                .field("len", &body.len())
                .finish(),
            Self::StaticSlice(body) => f
                .debug_struct("HotBody::StaticSlice")
                .field("len", &body.len())
                .finish(),
        }
    }
}
