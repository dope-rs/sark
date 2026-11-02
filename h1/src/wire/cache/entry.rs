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
        out: &mut [u8],
        date: &[u8; consts::DATE_LEN],
    ) -> Result<Option<Cached>, rc::Error> {
        let bytes = self.bytes.as_ref();
        let (len, cached) = if out.len() >= bytes.len() {
            (
                bytes.len(),
                Cached::Fixed {
                    len: bytes.len(),
                    body: bytes.len() - self.head,
                },
            )
        } else if out.len() >= self.head {
            // SAFETY: construction admits head within this immutable owner;
            // no API changes the owner or its validated visible extent.
            let body =
                unsafe { buffer::raw::Segment::try_project(&self.bytes, self.head..bytes.len()) }?;
            (
                self.head,
                Cached::Shared {
                    head: self.head,
                    body,
                },
            )
        } else {
            return Ok(None);
        };
        // SAFETY: the branch admits this output prefix, and construction admits
        // head within bytes. Exclusive out cannot overlap this immutable owner.
        unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), out.as_mut_ptr(), len) };
        if let Some(end) = self.date {
            // SAFETY: construction admits the complete date field within head;
            // both output paths cover head and retain the original exclusive out.
            unsafe {
                ptr::copy_nonoverlapping(
                    date.as_ptr(),
                    out.as_mut_ptr().add(end.get() - consts::DATE_LEN),
                    consts::DATE_LEN,
                );
            }
        }
        Ok(Some(cached))
    }
}
