use core::mem;

use sark_http::field::metadata::{map::record, section::storage};

use crate::{request::field, route::Schema};

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tag(u16);

impl Tag {
    pub const fn new(actual: u16) -> Self {
        Self(actual)
    }

    pub const fn actual(self) -> u16 {
        self.0
    }
}

pub trait Slot: Copy {
    fn into_tag(self) -> storage::Slot;

    fn from_tag(tag: storage::Slot) -> Result<Self, Tag>;
}

pub trait Policy<R: Schema>: Copy {
    fn prefix<P>(prefix: P) -> P::Output
    where
        P: field::Prefix<Self>;
}

pub trait Fields<R: Schema>: Slot {
    fn slice(name: &[u8]) -> Option<Self>;

    fn span(name: record::Span<'_>) -> Option<Self>;

    fn set<V: field::HeaderValue>(
        headers: &mut R::RawHeaders,
        slot: Self,
        value: &V,
    ) -> Result<(), R::Error>;
}

#[derive(Clone, Copy)]
pub enum Discard {}

impl Slot for Discard {
    fn into_tag(self) -> storage::Slot {
        match self {}
    }

    fn from_tag(tag: storage::Slot) -> Result<Self, Tag> {
        Err(Tag::new(tag.get() as u16))
    }
}

impl<R: Schema<Header = Self>> Policy<R> for Discard {
    fn prefix<P>(prefix: P) -> P::Output
    where
        P: field::Prefix<Self>,
    {
        prefix.miss(0)
    }
}

impl<R: Schema<Header = Self>> Fields<R> for Discard {
    fn slice(_: &[u8]) -> Option<Self> {
        None
    }

    fn span(_: record::Span<'_>) -> Option<Self> {
        None
    }

    fn set<V: field::HeaderValue>(
        _: &mut R::RawHeaders,
        slot: Self,
        _: &V,
    ) -> Result<(), R::Error> {
        match slot {}
    }
}

const _: () = {
    assert!(mem::size_of::<Tag>() == mem::size_of::<u16>());
    assert!(mem::size_of::<Discard>() == 0);
};
