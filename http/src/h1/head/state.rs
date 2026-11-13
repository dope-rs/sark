use core::convert::Infallible;

use crate::h1::{
    field,
    head::{Limit, ParseError, Semantic, line, tail::Tail},
};

pub(super) trait Kind: Clone + Copy {
    type Start: Clone + Copy;
    type Fields: Clone + Copy + Default;
    type Parts: Clone + Copy;

    fn start<E>(self, bytes: &[u8]) -> Result<line::Progress<Self::Start>, ParseError<E>>;

    fn fields(start: Self::Start) -> u32;

    fn observe(
        start: &Self::Start,
        fields: Self::Fields,
        field: &field::Entry<'_>,
    ) -> Result<Self::Fields, Semantic>;

    fn finish<E>(
        fields: Self::Fields,
        start: Self::Start,
        tail: Tail,
    ) -> Result<Self::Parts, ParseError<E>>;
}

/// Owner-free progress returned only after a successful parsing transition.
#[derive(Clone)]
pub(super) struct State<K: Kind> {
    phase: Phase<K>,
}

#[derive(Clone, Copy)]
enum Phase<K: Kind> {
    Start {
        kind: K,
        limit: Limit,
    },
    Line {
        kind: K,
        cursor: line::Cursor,
        limit: Limit,
    },
    Fields {
        cursor: field::Cursor,
        start: K::Start,
        fields: K::Fields,
        bytes: usize,
    },
    Done(K::Parts),
}

impl<K: Kind> State<K> {
    pub(super) const fn new(kind: K, limit: Limit) -> Self {
        Self {
            phase: Phase::Start { kind, limit },
        }
    }

    pub(super) fn parse<'owner, E>(
        self,
        owner: &'owner [u8],
        mut sink: impl FnMut(field::Entry<'owner>) -> Result<(), E>,
    ) -> Result<(Self, Option<K::Parts>), ParseError<E>> {
        let mut phase = self.phase;
        loop {
            match &mut phase {
                Phase::Start { kind, limit } => {
                    let bounded = owner.len().min(limit.bytes());
                    let progress = kind.start(&owner[..bounded])?;
                    match progress {
                        line::Progress::Complete(start) => {
                            phase = Self::fields(start, *limit).phase;
                        }
                        line::Progress::Partial(cursor) => {
                            phase = Phase::Line {
                                kind: *kind,
                                cursor,
                                limit: *limit,
                            };
                            if owner.len() == bounded {
                                break;
                            }
                        }
                    }
                }
                Phase::Line {
                    kind,
                    cursor,
                    limit,
                } => {
                    let (next, end) = cursor
                        .scan(owner, limit.bytes())
                        .map_err(ParseError::Boundary)?;
                    *cursor = next;
                    let Some(prefix) = end else {
                        break;
                    };
                    let progress = kind.start(prefix)?;
                    let line::Progress::Complete(start) = progress else {
                        return Err(ParseError::invalid());
                    };
                    phase = Self::fields(start, *limit).phase;
                }
                Phase::Fields {
                    cursor,
                    start,
                    fields,
                    bytes,
                } => {
                    let bounded = &owner[..owner.len().min(*bytes)];
                    let progress = cursor.advance(bounded).map_err(ParseError::Field)?;
                    let end = match progress {
                        field::Progress::End(end) => end,
                        field::Progress::Field(field) => {
                            let next =
                                K::observe(start, *fields, &field).map_err(ParseError::Semantic)?;
                            sink(field).map_err(ParseError::Sink)?;
                            *fields = next;
                            continue;
                        }
                        field::Progress::More if owner.len() > *bytes => {
                            return Err(ParseError::too_large(owner.len(), *bytes));
                        }
                        field::Progress::More => break,
                    };
                    let tail = Tail::new(K::fields(*start), end).map_err(ParseError::Boundary)?;
                    phase = Phase::Done(K::finish(*fields, *start, tail)?);
                    break;
                }
                Phase::Done(_) => break,
            }
        }
        let parts = match phase {
            Phase::Done(parts) => Some(parts),
            _ => None,
        };
        Ok((Self { phase }, parts))
    }

    pub(super) fn scan(
        self,
        owner: &[u8],
    ) -> Result<(Self, Option<K::Parts>), ParseError<Infallible>> {
        self.parse(owner, |_| Ok(()))
    }

    pub(super) fn fields(start: K::Start, limit: Limit) -> Self {
        Self {
            phase: Phase::Fields {
                cursor: field::Cursor::new(K::fields(start), limit.fields()),
                start,
                fields: K::Fields::default(),
                bytes: limit.bytes(),
            },
        }
    }
}
