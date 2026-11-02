use std::{num::NonZero, ops};

use o3::buffer::{storage, write::consume};

use crate::h1::head::request::{Route, trace};

pub struct Owned {
    pub(in crate::h1::head::request) route: Route<storage::Owned>,
    pub(in crate::h1::head::request) context: trace::Context,
    pub(in crate::h1::head::request) line: Option<trace::Line>,
}

impl Owned {
    pub fn context(&self) -> Option<trace::Context> {
        self.context.present()
    }

    pub fn into_source(self) -> storage::Owned {
        self.route.into_source()
    }

    pub fn with_parent(mut self, parent: &trace::Parent) -> Result<Self, consume::Error> {
        const NAME: &[u8] = b"traceparent: ";
        const EMPTY: [u8; trace::Parent::HEADER_BYTES] = [0; trace::Parent::HEADER_BYTES];
        const _: () = assert!(NAME.len() + trace::Parent::BYTES + 2 == trace::Parent::HEADER_BYTES);
        let (start, end) = match &self.line {
            Some(line) => ((line.start.get() - 1) as usize, line.end as usize),
            None => {
                let start = self.route.line.target.end() + 11;
                (start, start)
            }
        };
        let len = self.route.source.len();
        let growth = trace::Parent::HEADER_BYTES.saturating_sub(end - start);
        consume::Error::check(self.route.end.get() as usize + growth, u32::MAX as usize)?;
        let auth = self.route.line.auth;
        let shifted = if auth.start() >= end {
            let start = auth.start() + growth;
            Some(NonZero::<u32>::MIN.saturating_add(start as u32))
        } else {
            None
        };
        self.route.source.try_extend(&EMPTY[..growth])?;
        let stop = end + growth;
        let bytes = self.route.source.as_mut();
        // SAFETY: The admitted head retains `start <= end <= len` in this owner.
        // `try_extend` adds `growth` bytes, so `stop == end + growth` keeps
        // the tail copy of `len - end` bytes inside the same owner.
        // `field` spans `stop - start >= HEADER_BYTES == NAME.len() + BYTES + 2`,
        // so `NAME`, the parent bytes, and the trailing CRLF all fit, and
        // `stop - start - 2` cannot underflow. The checked head end plus growth
        // bounds all shifted spans in `u32`; the grown `route.end` stays nonzero.
        unsafe {
            let ptr = bytes.as_mut_ptr();
            ptr.add(end).copy_to(ptr.add(stop), len - end);
            let field = bytes.get_unchecked_mut(start..stop);
            field
                .get_unchecked_mut(NAME.len() + trace::Parent::BYTES..stop - start - 2)
                .fill(b' ');
            field.get_unchecked_mut(..NAME.len()).copy_from_slice(NAME);
            field
                .get_unchecked_mut(NAME.len()..NAME.len() + trace::Parent::BYTES)
                .copy_from_slice(&parent.to_bytes());
            field
                .get_unchecked_mut(stop - start - 2..)
                .copy_from_slice(b"\r\n");
            if let Some(shifted) = shifted {
                self.route.line.auth = auth.shifted_to(shifted);
            }
            self.route.end = NonZero::new_unchecked(self.route.end.get() + growth as u32);
        }
        self.context = parent.context;
        // SAFETY: The admitted `start` is before the nonzero head end, so
        // `start + 1` fits `u32`; the checked grown head end also bounds `stop`.
        self.line = Some(trace::Line {
            start: unsafe { NonZero::new_unchecked(start as u32 + 1) },
            end: stop as u32,
        });
        Ok(self)
    }
}

impl ops::Deref for Owned {
    type Target = Route<storage::Owned>;

    fn deref(&self) -> &Self::Target {
        &self.route
    }
}

const _: () = assert!(size_of::<Owned>() == 88);
