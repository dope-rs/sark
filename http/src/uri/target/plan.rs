use std::num::NonZero;

use o3::alloc::{self, array};

use crate::uri::target::{self, Absolute, Error, Slash, Target};

/// An admitted owned target whose canonical bytes are materialized on consumption.
/// Admission keeps the original buffer and checks every grammar and offset bound.
pub struct Plan {
    value: String,
    prefix: bool,
    slash: Slash,
    buffer: array::Plan<u8, usize>,
    bounds: Bounds,
}

pub(super) struct Bounds {
    scheme_end: Option<NonZero<u32>>,
    host_end: u32,
    authority_end: u32,
    path_end: u32,
}

impl Bounds {
    pub(super) fn new(scheme: usize, host: usize, end: usize, path: usize) -> Result<Self, Error> {
        Ok(Self {
            scheme_end: NonZero::new(target::narrow(scheme)?),
            host_end: target::narrow(host)?,
            authority_end: target::narrow(end)?,
            path_end: target::narrow(path)?,
        })
    }

    pub(super) fn build(self, value: Box<str>) -> Target {
        Target {
            value,
            scheme_end: self.scheme_end,
            host_end: self.host_end,
            authority_end: self.authority_end,
            path_end: self.path_end,
        }
    }
}

impl Plan {
    pub fn parse(value: String) -> Result<Self, Error> {
        let parsed = Absolute::parse(&value)?;
        let origin = parsed.origin();
        let scheme = parsed.scheme().len();
        let end = value.len() - origin.value.len();
        let host = scheme + 3 + parsed.authority().host().len();
        let path = end.checked_add(origin.path_end()).ok_or(Error::Length)?;
        let capacity = value
            .len()
            .checked_add(usize::from(origin.slash == Slash::Insert))
            .ok_or(Error::Length)?;
        let bounds = Bounds::new(scheme, host, end, path)?;
        let buffer = array::Plan::try_new(capacity).map_err(|_| Error::Length)?;
        let slash = origin.slash;
        Ok(Self {
            value,
            prefix: false,
            slash,
            buffer,
            bounds,
        })
    }

    /// Admits an HTTP authority with the root origin form.
    pub fn http(value: String) -> Result<Self, Error> {
        let parsed = target::authority(&value)?;
        let end = 7usize.checked_add(value.len()).ok_or(Error::Length)?;
        let capacity = end.checked_add(1).ok_or(Error::Length)?;
        let bounds = Bounds::new(4, 7 + parsed.host().len(), end, capacity)?;
        let buffer = array::Plan::try_new(capacity).map_err(|_| Error::Length)?;
        Ok(Self {
            value,
            prefix: true,
            slash: Slash::Insert,
            buffer,
            bounds,
        })
    }

    pub fn path(&self) -> &str {
        match self.slash {
            Slash::Insert => "/",
            Slash::Present => {
                let range = self.bounds.authority_end as usize..self.bounds.path_end as usize;
                // SAFETY: parse retains this ASCII path range in the unchanged owned input.
                unsafe { target::raw::slice(&self.value, range) }
            }
        }
    }

    /// Consumes the admitted offsets without parsing again. Exact capacity is
    /// reused; other capacities require one allocation and never a later shrink.
    pub fn try_build(self) -> Result<Target, alloc::Exhausted> {
        let mut value = self.value;
        if value.capacity() != *self.buffer.extent() {
            let mut output = self.buffer.try_string()?;
            if self.prefix {
                output.push_str("http://");
            }
            let end = self.bounds.authority_end as usize - output.len();
            // SAFETY: admission retains this authority split on an ASCII boundary of the input.
            let (head, tail) = unsafe {
                (
                    target::raw::slice(&value, 0..end),
                    target::raw::slice(&value, end..value.len()),
                )
            };
            output.push_str(head);
            if self.slash == Slash::Insert {
                output.push('/');
            }
            output.push_str(tail);
            value = output;
        } else {
            if self.prefix {
                // SAFETY: this plan's exact capacity admits its complete ASCII prefix and origin.
                unsafe { target::raw::insert(&mut value, 0, "http://") };
            }
            if self.slash == Slash::Insert {
                // SAFETY: the admitted ASCII authority boundary and spare root byte are unchanged.
                unsafe { target::raw::insert(&mut value, self.bounds.authority_end as usize, "/") };
            }
        }
        // SAFETY: reused or freshly allocated storage now contains every planned byte.
        Ok(self.bounds.build(unsafe { target::raw::boxed(value) }))
    }
}
