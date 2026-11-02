use mode::Active as _;
use sark_http::{
    self,
    field::metadata::{
        block, integer,
        strings::{Cursor, Encoding},
    },
    payload::compress::dynamic,
};

use crate::hpack::decode::{self, mode, strings};

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Bound(usize);

impl Bound {
    pub(super) const UNBOUNDED: Self = Self(usize::MAX);

    pub(super) const fn new(limit: Option<usize>) -> Self {
        match limit {
            Some(limit) => Self(limit),
            None => Self::UNBOUNDED,
        }
    }

    pub(super) fn option(self) -> Option<usize> {
        (self != Self::UNBOUNDED).then_some(self.0)
    }

    pub(super) fn admit(self, len: usize) -> Admission {
        if len <= self.0 {
            Admission::Admitted
        } else {
            Admission::Exceeded
        }
    }

    pub(super) fn after(self, amount: usize) -> Self {
        if self == Self::UNBOUNDED {
            self
        } else if amount >= self.0 {
            Self(0)
        } else {
            Self(self.0 - amount)
        }
    }

    pub(super) fn max(self, bounded: usize) -> Self {
        Self(self.0.max(bounded))
    }
}

pub(crate) struct Budget {
    pub(super) max: Bound,
    pub(super) used: usize,
    pub(crate) admission: Admission,
    resize: Resize,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Admission {
    Admitted,
    Exceeded,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Resize {
    Leading,
    Closed,
}

const _: () = {
    assert!(std::mem::size_of::<Admission>() == std::mem::size_of::<bool>());
    assert!(std::mem::size_of::<Resize>() == std::mem::size_of::<bool>());
    assert!(std::mem::size_of::<Budget>() == 3 * std::mem::size_of::<usize>());
};

impl Budget {
    pub(super) const fn new(max: Option<usize>) -> Self {
        Self {
            max: Bound::new(max),
            used: 0,
            admission: Admission::Admitted,
            resize: Resize::Leading,
        }
    }

    pub(super) fn close_resize(&mut self, required: Option<usize>) -> Result<(), decode::Error> {
        match self.resize {
            Resize::Closed => Ok(()),
            Resize::Leading => {
                if let Some(limit) = required {
                    return Err(decode::Error::MissingResize { limit });
                }
                self.resize = Resize::Closed;
                Ok(())
            }
        }
    }

    pub(super) fn admit_resize(&self, actual: usize, limit: usize) -> Result<(), decode::Error> {
        match self.resize {
            Resize::Closed => Err(decode::Error::LateResize),
            Resize::Leading if actual > limit => Err(decode::Error::Resize { actual, limit }),
            Resize::Leading => Ok(()),
        }
    }

    pub(super) fn retention(&self) -> Bound {
        self.max.after(self.used).after(dynamic::ENTRY_OVERHEAD)
    }

    pub(super) fn admit(&mut self, name_len: usize, value: usize) -> Admission {
        let used = self
            .used
            .checked_add(name_len)
            .and_then(|used| used.checked_add(value))
            .and_then(|used| used.checked_add(dynamic::ENTRY_OVERHEAD));
        let used = match used {
            Some(used) => used,
            None => {
                self.admission = Admission::Exceeded;
                return Admission::Exceeded;
            }
        };
        self.used = used;
        match self.max.admit(self.used) {
            Admission::Admitted => Admission::Admitted,
            Admission::Exceeded => {
                self.admission = Admission::Exceeded;
                Admission::Exceeded
            }
        }
    }

    pub(super) fn exceed(&mut self, name_len: usize, value: usize) {
        self.admit(name_len, value);
        self.admission = Admission::Exceeded;
    }
}

pub(crate) struct Literal<M> {
    retain_limit: Bound,
    mode: M,
    index: mode::Index,
}

pub(crate) struct Named<M: mode::Kind> {
    retain_limit: Bound,
    pub(super) mode: M::Active,
    retention: mode::NameRoute,
}

impl<M: mode::Kind> Literal<M> {
    pub(super) fn new(index: mode::Index, limit: Bound) -> Self {
        Self {
            retain_limit: limit,
            mode: M::new(),
            index,
        }
    }

    pub(super) fn begin<S>(
        self,
        sink: S,
        span: Option<block::Named<'_>>,
        len: usize,
    ) -> Result<(Named<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let Self {
            retain_limit: limit,
            mode,
            index,
        } = self;
        let retain = match &span {
            Some(_) => mode::NameRoute::Retain,
            None => mode::NameRoute::Count,
        };
        let name = mode::Name {
            span,
            facts: mode::Facts { len, index },
        };
        let (mode, sink) = mode.begin(sink, name)?;
        Ok((
            Named {
                retain_limit: limit,
                mode,
                retention: retain,
            },
            sink,
        ))
    }

    pub(super) fn retention(&self) -> Bound {
        match self.route() {
            mode::NameRoute::Retain => self.limit(),
            mode::NameRoute::Count => Bound::new(Some(0)),
        }
    }

    pub(super) fn limit(&self) -> Bound {
        self.retain_limit
    }

    pub(super) fn route(&self) -> mode::NameRoute {
        self.mode.name()
    }

    pub(super) fn index(&self) -> mode::Index {
        self.index
    }
}

impl<M: mode::Kind> Named<M> {
    pub(super) fn limit(&self) -> Bound {
        self.retain_limit.after(self.mode.facts().len)
    }

    pub(super) fn route(&self) -> mode::Route<M::Direct> {
        self.mode.route()
    }

    pub(super) fn index(&self) -> mode::Index {
        self.mode.facts().index
    }

    pub(super) fn retention(&self) -> mode::NameRoute {
        self.retention
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Integer {
    Indexed,
    LiteralName(mode::Index),
    DynamicSize,
}

#[repr(u8)]
pub(super) enum Progress {
    Complete,
    Pending,
}

const _: () = assert!(std::mem::size_of::<Progress>() == std::mem::size_of::<bool>());

pub(crate) struct Number(integer::Continuation);

pub(crate) struct LengthState {
    pub(super) number: Number,
    pub(super) encoding: Encoding,
}

impl Number {
    pub(super) const fn new(prefix: u64) -> Self {
        Self(integer::Continuation::new(prefix))
    }

    pub(super) const fn value(&self) -> u64 {
        self.0.value()
    }

    pub(super) fn feed(&mut self, input: &mut &[u8]) -> Result<Progress, decode::Error> {
        while let Some((&byte, rest)) = input.split_first() {
            *input = rest;
            if self.0.try_push(byte)?.is_break() {
                return Ok(Progress::Complete);
            }
        }
        Ok(Progress::Pending)
    }
}

pub(crate) struct StringState {
    pub(super) string: Cursor,
    pub(super) retention: strings::Retention,
    pub(super) validation: block::Value,
}
