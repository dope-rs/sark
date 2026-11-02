use std::mem;

use crate::{
    field::{self, scan::Match},
    h1::{field::Entry, head::request::offers::Source},
};

pub(in crate::h1) struct Iter<'a> {
    source: Source<'a>,
}

impl<'a> Iter<'a> {
    pub(super) const fn new(source: Source<'a>) -> Self {
        Self { source }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.source {
            Source::Compact(bytes) => {
                if bytes.is_empty() {
                    return None;
                }
                let Some(end) = Match::find(0, bytes) else {
                    return Some(mem::take(bytes));
                };
                *bytes = end.suffix();
                Some(end.prefix())
            }
            Source::Head(bytes) => {
                while !bytes.is_empty() {
                    let (name, value, rest) = Entry::split(bytes)?;
                    *bytes = rest;
                    if name.eq_ignore_ascii_case(b"upgrade") {
                        return Some(field::Error::trim_ows(value));
                    }
                }
                None
            }
        }
    }
}
