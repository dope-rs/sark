use std::{mem, ops};

use o3::{buffer::storage, mem::slice::Ext as _};

use crate::{
    Error, Nesting,
    segment::{Atom, Cursor, Projection},
    source::string::Scan,
};

mod name;
pub(super) use name::Name;

#[derive(Clone, Copy)]
pub(crate) enum ValueKind {
    String { escapes: usize },
    Unsigned(u64),
    Negative(u64),
    Number,
    True,
    False,
    Null,
    Compound,
}

impl ValueKind {
    pub(crate) fn i64(self) -> crate::Result<i64> {
        match self {
            Self::Unsigned(value) => i64::try_from(value).map_err(|_| Error::Invalid),
            Self::Negative(value) => 0_i64.checked_sub_unsigned(value).ok_or(Error::Invalid),
            _ => Err(Error::Invalid),
        }
    }
}

const _: () = assert!(mem::size_of::<ValueKind>() == 2 * mem::size_of::<usize>());

pub(super) enum Stored {
    None,
    Bytes(ops::Range<usize>),
    Frame(ops::Range<usize>),
    Segments(ops::Range<usize>),
    Copied(storage::Shared),
    Decoded(storage::Shared),
}

pub(super) struct Value {
    pub(super) stored: Stored,
    pub(super) kind: ValueKind,
}

impl Value {
    pub(super) fn is_retained(&self, segment: &ops::Range<usize>) -> bool {
        match &self.stored {
            Stored::Bytes(range) | Stored::Frame(range) | Stored::Segments(range) => {
                is_overlap(range, segment)
            }
            _ => false,
        }
    }
}

pub(super) struct Capture {
    field: Option<usize>,
    mode: Projection,
    start: usize,
}

impl Capture {
    pub(super) fn new(field: Option<usize>, mode: Projection, cursor: &Cursor) -> Self {
        Self {
            field,
            mode,
            start: cursor.absolute,
        }
    }

    pub(super) fn is_original(&self) -> bool {
        self.field.is_some()
            && matches!(
                self.mode,
                Projection::Bytes | Projection::Frame | Projection::Segments
            )
    }

    pub(super) fn start(&self) -> usize {
        self.start
    }

    pub(super) fn finish(self, end: usize, kind: ValueKind) -> Option<(usize, Value)> {
        let field = self.field?;
        Some((
            field,
            Value {
                stored: match self.mode {
                    Projection::None | Projection::Scalar => Stored::None,
                    Projection::Bytes => Stored::Bytes(self.start..end),
                    Projection::Frame => Stored::Frame(self.start..end),
                    Projection::Segments => Stored::Segments(self.start..end),
                },
                kind,
            },
        ))
    }
}

pub(super) enum State {
    Leading,
    NameOrEnd,
    NameStart,
    Name {
        start: usize,
        string: Scan,
    },
    Colon {
        name: ops::Range<usize>,
        escaped: bool,
    },
    Select(Name),
    ValueStart {
        field: Option<usize>,
        projection: Projection,
    },
    String {
        capture: Capture,
        string: Scan,
    },
    Compound {
        capture: Capture,
        nesting: Nesting,
    },
    Atom {
        capture: Capture,
        atom: Atom,
    },
    Trailing,
    Done,
}

pub(super) enum Pin {
    Free,
    Range(ops::Range<usize>),
}

impl State {
    pub(super) const fn is_done(&self) -> bool {
        matches!(self, Self::Done)
    }

    pub(super) const fn can_wait(&self) -> bool {
        match self {
            Self::Leading
            | Self::NameOrEnd
            | Self::NameStart
            | Self::Name { .. }
            | Self::Colon { .. }
            | Self::ValueStart { .. }
            | Self::String { .. }
            | Self::Compound { .. }
            | Self::Atom { .. }
            | Self::Trailing => true,
            Self::Select(..) | Self::Done => false,
        }
    }

    /// Returns only a prefix whose grammar has been validated and whose bytes
    /// leave the current state unchanged.
    pub(super) fn skip<'a>(&self, chunk: &'a [u8]) -> &'a [u8] {
        match self {
            Self::Leading
            | Self::NameOrEnd
            | Self::NameStart
            | Self::Colon { .. }
            | Self::ValueStart { .. }
            | Self::Trailing => {
                chunk
                    .split_while(|byte| crate::input::is_whitespace(*byte))
                    .0
            }
            Self::Name { string, .. } | Self::String { string, .. } => string.skip(chunk),
            Self::Compound { nesting, .. } => nesting.skip(chunk),
            Self::Select(..) | Self::Atom { .. } | Self::Done => &chunk[..0],
        }
    }

    pub(super) fn pin(&self, end: usize) -> Pin {
        match self {
            Self::Name { start, .. } => Pin::Range(*start..end),
            Self::Colon { name, .. } => Pin::Range(name.clone()),
            Self::Select(name) => Pin::Range(name.range()),
            Self::String { capture, .. }
            | Self::Compound { capture, .. }
            | Self::Atom { capture, .. } => {
                if capture.is_original() {
                    Pin::Range(capture.start()..end)
                } else {
                    Pin::Free
                }
            }
            Self::ValueStart { .. }
            | Self::Leading
            | Self::NameOrEnd
            | Self::NameStart
            | Self::Trailing
            | Self::Done => Pin::Free,
        }
    }
}

pub(super) enum ScanProgress {
    NeedInput,
    Pending,
    Ready,
}

pub(super) fn is_delimiter(byte: u8) -> bool {
    crate::input::is_whitespace(byte) || matches!(byte, b',' | b'}' | b']')
}

pub(super) fn is_overlap(left: &ops::Range<usize>, right: &ops::Range<usize>) -> bool {
    left.start < right.end && right.start < left.end
}
