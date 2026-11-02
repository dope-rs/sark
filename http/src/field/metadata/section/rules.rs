//! Validation and retention rules for compressed field sections.

use core::{convert::Infallible, ops};

use state::{complete, decode, fixed};

use crate::field::{
    head,
    metadata::{
        block,
        map::record,
        packed, section,
        section::{state, storage},
    },
};

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
#[derive(Debug)]
pub enum Kind {
    Request,
    Response,
    RequestTrailers,
    ResponseTrailers,
    PushPromise,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum Retention {
    Retain,
    Discard,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum Inspect {
    Method,
}

pub trait Push {
    const SCHEMES: Option<&'static [&'static [u8]]>;
    const AUTHORITIES: Option<&'static [&'static [u8]]>;

    fn is_scheme(value: record::Span<'_>) -> bool {
        Self::SCHEMES.is_none_or(|allowed| allowed.iter().any(|expected| value.is_equal(expected)))
    }

    fn is_authority(value: record::Span<'_>) -> bool {
        Self::AUTHORITIES
            .is_none_or(|allowed| allowed.iter().any(|expected| value.is_equal(expected)))
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct RejectPush;

impl Push for RejectPush {
    const SCHEMES: Option<&'static [&'static [u8]]> = Some(&[]);
    const AUTHORITIES: Option<&'static [&'static [u8]]> = Some(&[]);
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Disposition<I = Infallible> {
    Discard,
    #[doc(hidden)]
    Inspect(I),
    Tagged(storage::Tag),
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum Decision<I = Infallible> {
    Discard(head::Name),
    Inspect(I, head::Name),
    Skip,
    Scratch,
    Tagged(head::Name, storage::Tag),
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum Store {
    Scratch,
    Tagged(storage::Tag),
}

pub struct Candidate<'a, P: ?Sized> {
    policy: &'a mut P,
    name: record::Span<'a>,
    known: head::Name,
}

pub struct Observation<'a, P: ?Sized> {
    policy: &'a mut P,
    field: record::View<'a>,
    retained: &'a [u8],
}

pub struct Commit<'a, P: ?Sized> {
    policy: &'a mut P,
    tag: storage::Tag,
    range: ops::Range<usize>,
    retained: &'a [u8],
}

impl<'a, P: ?Sized> Candidate<'a, P> {
    pub(super) fn new(policy: &'a mut P, name: record::Span<'a>, known: head::Name) -> Self {
        Self {
            policy,
            name,
            known,
        }
    }

    pub fn name(&self) -> record::Span<'a> {
        self.name
    }

    pub fn identify(&self) -> (record::Span<'a>, head::Name) {
        (self.name, self.known)
    }

    pub fn classify(self) -> (&'a mut P, record::Span<'a>, head::Name) {
        (self.policy, self.name, self.known)
    }
}

impl<'a, P: ?Sized> Observation<'a, P> {
    pub(super) fn new(policy: &'a mut P, field: record::View<'a>, retained: &'a [u8]) -> Self {
        Self {
            policy,
            field,
            retained,
        }
    }

    pub fn into_parts(self) -> (&'a mut P, record::View<'a>, &'a [u8]) {
        (self.policy, self.field, self.retained)
    }
}

impl<'a, P: ?Sized> Commit<'a, P> {
    pub(super) fn new(
        policy: &'a mut P,
        tag: storage::Tag,
        range: ops::Range<usize>,
        retained: &'a [u8],
    ) -> Self {
        Self {
            policy,
            tag,
            range,
            retained,
        }
    }

    pub fn into_parts(self) -> (&'a mut P, storage::Tag, ops::Range<usize>, &'a [u8]) {
        (self.policy, self.tag, self.range, self.retained)
    }

    pub fn store(self) -> (&'a mut P, storage::Tag, ops::Range<usize>) {
        (self.policy, self.tag, self.range)
    }
}

pub trait Policy: section::Policy {
    #[doc(hidden)]
    type State: decode::Sink<Self> + complete::Sink<Self>;
}

/// Applies value validity carried by a decoded or indexed field.
pub trait Validate {
    fn value(&mut self, check: block::Validation);
}

pub trait Plan: section::Policy<Block = storage::Fields> + Default {
    type Inspect: Copy;
    const SKIP_DISCARDED: bool;

    fn disposition(input: Candidate<'_, Self>) -> Disposition<Self::Inspect>;
    fn inspect(token: Self::Inspect, input: Observation<'_, Self>) -> block::Validation;
    fn commit(input: Commit<'_, Self>) -> block::Validation;
}

impl<P: Plan> Policy for P {
    type State = state::Decoder<P>;
}

#[derive(Clone, Copy, Default)]
pub struct RetainRaw;

impl section::Policy for RetainRaw {
    type Selection = ();
    type Block = packed::Decoded;

    fn finish(self) -> Self::Selection {
        match self {
            Self => (),
        }
    }
}

impl Policy for RetainRaw {
    type State = fixed::Raw;
}

#[derive(Clone, Copy, Default)]
pub struct Discard;

impl section::Policy for Discard {
    type Selection = ();
    type Block = storage::Fields;

    fn finish(self) -> Self::Selection {
        match self {
            Self => (),
        }
    }
}

impl Policy for Discard {
    type State = fixed::Discard;
}

const _: () = {
    assert!(core::mem::size_of::<RejectPush>() == 0);
    assert!(core::mem::size_of::<Retention>() == 1);
    assert!(core::mem::size_of::<Store>() == core::mem::size_of::<Decision>());
};
