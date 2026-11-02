use core::{convert::Infallible, ops};

use crate::h1::{
    field,
    head::{Limit, ParseError, Semantic, line, tail::Tail},
};

pub(super) trait Kind: Clone + Copy {
    type Start: Clone + Copy;
    type Fields: Clone + Copy + Default;
    type Parts: Clone + Copy;

    fn start<E>(bytes: &[u8]) -> Result<line::Progress<Self::Start>, ParseError<E>>;

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
        limit: Limit,
    },
    Line {
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
    pub(super) fn pending_line(owner: &[u8], limit: Limit) -> Result<Self, ParseError<Infallible>> {
        if owner.len() > limit.bytes() {
            return Err(ParseError::too_large(owner.len(), limit.bytes()));
        }
        let cursor = line::Cursor::pending(owner).map_err(|_| ParseError::invalid())?;
        Ok(Self {
            phase: Phase::Line { cursor, limit },
        })
    }

    pub(super) fn advance_line(self, owner: &[u8]) -> Result<(Self, bool), ParseError<Infallible>> {
        let Phase::Line { cursor, limit } = self.phase else {
            return Err(ParseError::invalid());
        };
        let (cursor, end) = cursor
            .scan(owner, limit.bytes())
            .map_err(ParseError::Boundary)?;
        Ok((
            Self {
                phase: Phase::Line { cursor, limit },
            },
            end.is_some(),
        ))
    }

    pub(super) const fn new(limit: Limit) -> Self {
        Self {
            phase: Phase::Start { limit },
        }
    }

    pub(super) fn parse<'owner, E>(
        self,
        owner: &'owner [u8],
        mut sink: impl FnMut(field::Entry<'owner>) -> Result<(), E>,
    ) -> Result<(Self, Option<K::Parts>), ParseError<E>> {
        let phase = Self::run(self.phase, owner, |start, fields, field| {
            let fields = K::observe(start, fields, &field).map_err(ParseError::Semantic)?;
            sink(field).map_err(ParseError::Sink)?;
            Ok(fields)
        })?;
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

    fn run<'owner, E>(
        mut phase: Phase<K>,
        owner: &'owner [u8],
        mut drain: impl FnMut(
            &K::Start,
            K::Fields,
            field::Entry<'owner>,
        ) -> Result<K::Fields, ParseError<E>>,
    ) -> Result<Phase<K>, ParseError<E>> {
        loop {
            match phase {
                Phase::Start { limit } => {
                    let bounded = owner.len().min(limit.bytes());
                    let progress = K::start(&owner[..bounded])?;
                    match progress {
                        line::Progress::Complete(start) => {
                            phase = Self::fields(start, limit).phase;
                        }
                        line::Progress::Partial(cursor) => {
                            let next = Phase::Line { cursor, limit };
                            if owner.len() == bounded {
                                return Ok(next);
                            }
                            phase = next;
                        }
                    }
                }
                Phase::Line { cursor, limit } => {
                    let (cursor, end) = cursor
                        .scan(owner, limit.bytes())
                        .map_err(ParseError::Boundary)?;
                    let Some(prefix) = end else {
                        return Ok(Phase::Line { cursor, limit });
                    };
                    let progress = K::start(prefix)?;
                    let line::Progress::Complete(start) = progress else {
                        return Err(ParseError::invalid());
                    };
                    phase = Self::fields(start, limit).phase;
                }
                Phase::Fields {
                    mut cursor,
                    start,
                    mut fields,
                    bytes,
                } => {
                    let bounded = &owner[..owner.len().min(bytes)];
                    let parsed =
                        cursor.advance(bounded, |field| match drain(&start, fields, field) {
                            Ok(next) => {
                                fields = next;
                                ops::ControlFlow::Continue(())
                            }
                            Err(error) => ops::ControlFlow::Break(error),
                        });
                    let progress = parsed.map_err(ParseError::Field)?;
                    let end = match progress {
                        field::Progress::Break(error) => return Err(error),
                        field::Progress::End(end) => end,
                        field::Progress::Emitted(()) => {
                            phase = Phase::Fields {
                                cursor,
                                start,
                                fields,
                                bytes,
                            };
                            continue;
                        }
                        field::Progress::More if owner.len() > bytes => {
                            return Err(ParseError::too_large(owner.len(), bytes));
                        }
                        field::Progress::More => {
                            return Ok(Phase::Fields {
                                cursor,
                                start,
                                fields,
                                bytes,
                            });
                        }
                    };
                    let tail = Tail::new(K::fields(start), end).map_err(ParseError::Boundary)?;
                    let parts = K::finish(fields, start, tail)?;
                    return Ok(Phase::Done(parts));
                }
                Phase::Done(parts) => return Ok(Phase::Done(parts)),
            }
        }
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
