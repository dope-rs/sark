use sark_http::field::{Indexing, metadata::map::record::Entry};

use crate::protocol::section::Valid;

mod raw;

/// An immutable suffix of the complete field sequence admitted by `Valid`.
#[derive(Clone, Copy)]
struct Fields<'a>(&'a [u8]);

/// A nonempty sequence of part prefixes written by `Fold::joined`.
#[derive(Clone, Copy)]
struct Parts<'a>(&'a [u8]);

enum Value<'a> {
    Plain(&'a [u8]),
    Joined(Parts<'a>),
}

enum Tail<'a> {
    More(Parts<'a>),
    Last(&'a [u8]),
}

#[derive(Clone)]
enum State<'a> {
    Fields(Fields<'a>),
    Joined {
        fields: Fields<'a>,
        name: &'a [u8],
        indexing: Indexing,
        parts: Parts<'a>,
    },
    Final {
        fields: Fields<'a>,
        name: &'a [u8],
        indexing: Indexing,
        value: &'a [u8],
    },
}

#[derive(Clone)]
pub(crate) struct Iter<'a> {
    state: State<'a>,
}

impl<'a> Iter<'a> {
    pub(crate) fn from(owner: &'a Valid) -> Self {
        Self {
            state: State::Fields(Fields::new(owner)),
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.state {
                State::Fields(current) => {
                    let (name, value, indexing) = current.next()?;
                    match value {
                        Value::Plain(value) => {
                            return Some(Entry::new(name, value).with_index(indexing));
                        }
                        Value::Joined(parts) => {
                            self.state = State::Joined {
                                fields: *current,
                                name,
                                indexing,
                                parts,
                            };
                        }
                    }
                }
                State::Joined {
                    fields,
                    name,
                    indexing,
                    parts,
                } => {
                    let fields = *fields;
                    let name = *name;
                    let indexing = *indexing;
                    let (value, tail) = parts.take();
                    match tail {
                        Tail::Last(rest) => {
                            self.state = State::Final {
                                fields,
                                name,
                                indexing,
                                value: rest,
                            };
                        }
                        Tail::More(rest) => *parts = rest,
                    }
                    return Some(Entry::new(name, value).with_index(indexing));
                }
                State::Final {
                    fields,
                    name,
                    indexing,
                    value,
                } => {
                    let fields = *fields;
                    let name = *name;
                    let indexing = *indexing;
                    let value = *value;
                    let field = Entry::new(name, value).with_index(indexing);
                    self.state = State::Fields(fields);
                    return Some(field);
                }
            }
        }
    }
}

const _: () = assert!(size_of::<Iter<'static>>() == 7 * size_of::<usize>());
