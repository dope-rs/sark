use o3::buffer::write::{Sink as _, Slice};
use std::{num::NonZero, ptr};

use o3::{
    buffer::{self, storage},
    mem::{Rejected, rc},
};

use crate::wire::{
    cache::{Cached, Error},
    response::{Context, consts},
};

pub struct Entry {
    bytes: storage::Shared,
    date: Option<NonZero<usize>>,
    head: usize,
    response: Context,
}

impl Entry {
    /// Admits the body split and complete date field within this owner's head.
    pub fn try_new(
        bytes: storage::Shared,
        date: Option<usize>,
        body: usize,
        response: Context,
    ) -> Result<Self, Rejected<storage::Shared, Error>> {
        let len = bytes.len();
        if body > len {
            return Err(Rejected::new(
                bytes,
                Error::Body {
                    length: body,
                    available: len,
                },
            ));
        }
        let head = len - body;
        let date = match date {
            Some(offset) => match NonZero::<usize>::MIN
                .saturating_add(consts::DATE_LEN - 1)
                .checked_add(offset)
            {
                Some(end) if end.get() <= head => Some(end),
                _ => {
                    return Err(Rejected::new(
                        bytes,
                        Error::Date {
                            offset,
                            available: head,
                        },
                    ));
                }
            },
            None => None,
        };
        Ok(Self {
            bytes,
            date,
            head,
            response,
        })
    }

    pub(super) fn matches(&self, response: Context) -> bool {
        self.response == response
    }

    pub fn try_write(
        &self,
        mut out: Slice<'_, &mut usize>,
        date: &[u8; consts::DATE_LEN],
    ) -> Result<Option<Cached>, rc::Error> {
        let bytes = self.bytes.as_ref();
        let (len, cached) = if out.remaining() >= bytes.len() {
            (
                bytes.len(),
                Cached::Fixed {
                    body: bytes.len() - self.head,
                },
            )
        } else if out.remaining() >= self.head {
            // SAFETY: construction admits head within this immutable owner;
            // no API changes the owner or its validated visible extent.
            let body =
                unsafe { buffer::raw::Segment::try_project(&self.bytes, self.head..bytes.len()) }?;
            (self.head, Cached::Shared { body })
        } else {
            return Ok(None);
        };
        let start = out.len();
        out.write_slice(&bytes[..len])
            .unwrap_or_else(|_| unreachable!("capacity"));
        let out = out.into_mut();
        if let Some(end) = self.date {
            // SAFETY: construction admits the complete date field within head;
            // both output paths cover head and retain the original exclusive out.
            unsafe {
                ptr::copy_nonoverlapping(
                    date.as_ptr(),
                    out.as_mut_ptr().add(start + end.get() - consts::DATE_LEN),
                    consts::DATE_LEN,
                );
            }
        }
        Ok(Some(cached))
    }
}
