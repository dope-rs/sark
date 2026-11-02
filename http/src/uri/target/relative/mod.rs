use std::iter;

use o3::alloc::array;

use crate::uri::{
    path,
    target::{self, Error, Origin, Slash, Target},
};

mod raw;

/// An admitted relative or origin-form path and optional query.
#[derive(Clone, Copy)]
pub struct Relative<'a> {
    value: &'a str,
    path_end: u32,
}

impl<'a> Relative<'a> {
    pub(super) fn from_target(target: &'a Target) -> Self {
        Self {
            value: target.path_query(),
            path_end: target.path_end - target.authority_end,
        }
    }

    pub fn parse(value: &'a str) -> Result<Self, Error> {
        let plan = path::Scan::new(value.as_bytes())
            .complete()
            .map_err(Error::Origin)?;
        Ok(Self {
            value,
            path_end: target::narrow(plan.path_end())?,
        })
    }

    /// Resolves only admitted components; normalization cannot introduce new bytes
    /// other than path separators, so the original grammar proof remains valid.
    /// Materialization precedes offset narrowing.
    pub fn resolve(self, base: &Target) -> Result<Target, Error> {
        if self.value.is_empty() {
            return base.try_clone().map_err(Error::Allocation);
        }
        if self.value.starts_with('/') && !self.has_dots() {
            return base.with_origin(Origin {
                value: self.value,
                path_end: self.path_end,
                slash: Slash::Present,
            });
        }
        self.materialize(base)
    }

    pub(super) fn materialize(self, base: &Target) -> Result<Target, Error> {
        // SAFETY: complete admission retains the ASCII path boundary in these immutable bytes.
        let (path, query) = unsafe {
            (
                target::raw::slice(self.value, 0..self.path_end as usize),
                target::raw::slice(self.value, self.path_end as usize..self.value.len()),
            )
        };
        let (value, path_end) = if path.is_empty() {
            let capacity = (base.path_end as usize)
                .checked_add(query.len())
                .ok_or(Error::Length)?;
            let mut value = target::buffer(capacity)?;
            // SAFETY: the base owns this admitted ASCII path prefix without mutation.
            value.push_str(unsafe { target::raw::slice(&base.value, 0..base.path_end as usize) });
            value.push_str(query);
            (value, base.path_end)
        } else {
            let (prefix, path) = match path.strip_prefix('/') {
                Some(path) => ("", path),
                None => {
                    let base = base.path();
                    let end = base.rfind('/').unwrap_or(0) + 1;
                    // SAFETY: the admitted path starts with '/' and rfind locates its final separator.
                    (unsafe { target::raw::slice(base, 1..end) }, path)
                }
            };
            let segments = segments(prefix, path);
            let path_len = segments.clone().try_fold(0usize, |len, part| {
                len.checked_add(part.len() + 1).ok_or(Error::Length)
            })?;
            let path_end = (base.authority_end as usize)
                .checked_add(path_len)
                .ok_or(Error::Length)?;
            let capacity = path_end.checked_add(query.len()).ok_or(Error::Length)?;
            let buffer = array::Plan::try_new(capacity).map_err(|_| Error::Length)?;
            // SAFETY: the clone measured this same deterministic iterator over
            // immutable segments, including one preceding slash per segment.
            let value = unsafe {
                raw::normalize(
                    buffer,
                    target::raw::slice(&base.value, 0..base.authority_end as usize),
                    path_len,
                    segments,
                    query,
                )
            }
            .map_err(Error::Allocation)?;
            target::narrow(path_len)?;
            let path_end = target::narrow(path_end)?;
            (value, path_end)
        };
        Ok(Target {
            // SAFETY: both branches initialize every byte of their exact-capacity allocation.
            value: unsafe { target::raw::boxed(value) },
            scheme_end: base.scheme_end,
            host_end: base.host_end,
            authority_end: base.authority_end,
            path_end,
        })
    }

    pub(super) fn has_dots(self) -> bool {
        // SAFETY: admission retains this ASCII path prefix on the unchanged borrowed bytes.
        unsafe { target::raw::slice(self.value, 0..self.path_end as usize) }
            .split('/')
            .any(|part| matches!(part, "." | ".."))
    }
}

/// Yields normalized segments from the end without storing or joining them.
/// `prefix` excludes the root slash and ends at a directory separator.
fn segments<'a>(prefix: &'a str, path: &'a str) -> impl Iterator<Item = &'a str> + Clone {
    let trailing = matches!(path.rsplit('/').next(), Some("." | ".."));
    let mut parents = 0usize;
    iter::once("").take(usize::from(trailing)).chain(
        path.rsplit('/')
            .chain(prefix.rsplit_terminator('/'))
            .filter(move |part| match *part {
                "." => false,
                ".." => {
                    parents += 1;
                    false
                }
                _ if parents != 0 => {
                    parents -= 1;
                    false
                }
                _ => true,
            }),
    )
}
