use std::{collections::TryReserveError, mem, num::NonZero, str};

use sark_http::{
    method::Method,
    payload::response::status,
    uri::{self, target},
};

#[derive(Debug)]
pub enum Error {
    Uri(target::Error),
    Parts(target::Error),
    MissingScheme,
    Location,
    LocationUtf8(str::Utf8Error),
    Allocation(TryReserveError),
    Limit,
    Loop,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Relation {
    Same,
    Cross,
}

use crate::{Failure, Safety};

fn possibly_committed<E, M>(cause: crate::Error<E, M>) -> Failure<E, M> {
    Failure::new(cause, Safety::PossiblyCommitted)
}

fn origin_relation(a: &target::Target, b: &target::Target) -> Relation {
    let same = a
        .scheme_str()
        .zip(b.scheme_str())
        .is_some_and(|(left, right)| {
            let scheme = if left.eq_ignore_ascii_case("http") {
                uri::Scheme::Http
            } else if left.eq_ignore_ascii_case("https") {
                uri::Scheme::Https
            } else {
                uri::Scheme::Other
            };
            left.eq_ignore_ascii_case(right) && a.parts().is_same(b.parts(), scheme)
        });
    if same {
        Relation::Same
    } else {
        Relation::Cross
    }
}

fn resolve(base: &target::Target, location: &str) -> Result<target::Target, crate::Error> {
    let location = location.split_once('#').map_or(location, |split| split.0);
    if location.is_empty() {
        return base
            .try_clone()
            .map_err(target::Error::Allocation)
            .map_err(Error::Parts)
            .map_err(crate::Error::Redirect);
    }
    if let Some(scheme) = location
        .split_inclusive([':', '/', '?'])
        .next()
        .and_then(|part| part.strip_suffix(':'))
    {
        if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
            uri::Scheme::parse(scheme.as_bytes())
                .map_err(target::Error::Scheme)
                .map_err(Error::Uri)
                .map_err(crate::Error::Redirect)?;
            return Err(crate::Error::Origin(crate::OriginError::UnsupportedScheme));
        }
        return absolute(target::Target::parse(location));
    }
    if location.starts_with("//") {
        let scheme = base
            .scheme_str()
            .ok_or(crate::Error::Redirect(Error::MissingScheme))?;
        let capacity = scheme
            .len()
            .checked_add(1)
            .and_then(|len| len.checked_add(location.len()))
            .ok_or(crate::Error::Redirect(Error::Uri(target::Error::Length)))?;
        let mut absolute = String::new();
        absolute
            .try_reserve_exact(capacity)
            .map_err(Error::Allocation)
            .map_err(crate::Error::Redirect)?;
        absolute.push_str(scheme);
        absolute.push(':');
        absolute.push_str(location);
        return self::absolute(match target::Plan::parse(absolute) {
            Ok(plan) => plan.try_build().map_err(target::Error::Allocation),
            Err(error) => Err(error),
        });
    }

    target::Relative::parse(location)
        .map_err(|error| crate::Error::Redirect(Error::Uri(error)))?
        .resolve(base)
        .map_err(|error| crate::Error::Redirect(Error::Parts(error)))
}

fn absolute(target: Result<target::Target, target::Error>) -> Result<target::Target, crate::Error> {
    let target = target.map_err(Error::Uri).map_err(crate::Error::Redirect)?;
    target
        .normalize()
        .map_err(|error| crate::Error::Redirect(Error::Parts(error)))
}

fn resolve_redirect(
    current: &target::Target,
    location: &str,
) -> Result<target::Target, crate::Error> {
    let next = resolve(current, location)?;
    if origin_relation(current, &next) == Relation::Cross {
        return Err(crate::Error::Request(crate::RequestError::CrossOrigin));
    }
    Ok(next)
}

enum Current<'a> {
    Borrowed(&'a str),
    Owned {
        visited: History,
        relation: Relation,
    },
}

// Every appended target has passed resolve_redirect against the current origin.
struct History {
    current: target::Target,
    prior: Vec<target::Target>,
}

impl History {
    fn try_new(capacity: NonZero<usize>, first: target::Target) -> Result<Self, crate::Error> {
        let mut prior = Vec::new();
        prior
            .try_reserve_exact(capacity.get() - 1)
            .map_err(Error::Allocation)
            .map_err(crate::Error::Redirect)?;
        Ok(Self {
            current: first,
            prior,
        })
    }

    fn contains(&self, target: &target::Target, prior: u32) -> bool {
        let path = target.path_query();
        self.current.path_query() == path
            || self
                .prior
                .iter()
                .rev()
                .take(prior as usize)
                .any(|prior| prior.path_query() == path)
    }

    fn try_push(&mut self, target: target::Target) -> Result<(), crate::Error> {
        self.prior
            .try_reserve(1)
            .map_err(Error::Allocation)
            .map_err(crate::Error::Redirect)?;
        self.prior.push(mem::replace(&mut self.current, target));
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Rewrite {
    Preserve,
    Get,
}

impl Rewrite {
    fn select(status: status::Code, method: &Method) -> Option<Self> {
        match status {
            status::Code::MOVED_PERMANENTLY | status::Code::FOUND if *method == Method::POST => {
                Some(Self::Get)
            }
            status::Code::MOVED_PERMANENTLY | status::Code::FOUND => Some(Self::Preserve),
            status::Code::SEE_OTHER if *method == Method::HEAD => Some(Self::Preserve),
            status::Code::SEE_OTHER => Some(Self::Get),
            status::Code::TEMPORARY_REDIRECT | status::Code::PERMANENT_REDIRECT => {
                Some(Self::Preserve)
            }
            _ => None,
        }
    }
}

/// The transport-independent result of applying one response to a redirect
/// transaction.
pub enum Action<'a> {
    Complete,
    Follow(Machine<'a>),
}

/// A borrowed request transaction whose redirect policy is independent of
/// the connector used to dispatch it.
pub struct Machine<'a> {
    target: State<'a>,
    method: Method,
    body: Option<&'a [u8]>,
}

impl<'a> Machine<'a> {
    pub fn new(
        limit: u32,
        base: &'a target::Target,
        path: &'a str,
        method: Method,
        body: &'a [u8],
    ) -> Result<Self, crate::Error> {
        Ok(Self {
            target: State::new(limit, base, path)?,
            method,
            body: Some(body),
        })
    }

    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn body(&self) -> &'a [u8] {
        self.body.unwrap_or_default()
    }

    /// Whether redirects have retained the original content and its metadata.
    pub fn preserves_body(&self) -> bool {
        self.body.is_some()
    }

    pub fn origin(&self) -> &target::Target {
        self.target.origin()
    }

    pub fn is_base(&self) -> bool {
        self.target.is_base()
    }

    #[doc(hidden)]
    pub fn safety(&self) -> Safety {
        if self.target.has_prior() {
            Safety::PossiblyCommitted
        } else {
            Safety::BeforeSubmit
        }
    }

    /// Applies the complete HTTP redirect decision at one response boundary.
    /// Statuses outside 301, 302, 303, 307, and 308 are final responses.
    /// Looks up the location only when that status requires a redirect.
    /// Redirects to another origin fail before any request is replayed.
    pub fn response<'head>(
        self,
        status: status::Code,
        location: impl FnOnce() -> Option<&'head [u8]>,
    ) -> Result<Action<'a>, Failure> {
        let Some(rewrite) = Rewrite::select(status, &self.method) else {
            return Ok(Action::Complete);
        };
        let location = location()
            .ok_or(crate::Error::Redirect(Error::Location))
            .map_err(possibly_committed)?;
        let location = str::from_utf8(location)
            .map_err(Error::LocationUtf8)
            .map_err(crate::Error::Redirect)
            .map_err(possibly_committed)?;
        let changed = matches!(rewrite, Rewrite::Get) && self.method != Method::GET;
        let target = self
            .target
            .advance(location, changed)
            .map_err(possibly_committed)?;
        let (method, body) = match rewrite {
            Rewrite::Preserve => (self.method, self.body),
            Rewrite::Get => (Method::GET, None),
        };
        Ok(Action::Follow(Self {
            target,
            method,
            body,
        }))
    }
}

impl Target for Machine<'_> {
    fn origin_form(&self) -> &str {
        self.target.origin_form()
    }
}

/// An HTTP request target that exposes its origin-form path without changing
/// ownership.
pub trait Target {
    fn origin_form(&self) -> &str;
}

/// Redirect state that borrows the initial target until a redirect requires
/// owned URI and loop-history storage.
pub struct State<'a> {
    limit: u32,
    /// Redirects preserve the method or change it once to GET. This phase's
    /// initial budget stays >= limit; the difference counts its history suffix.
    phase_limit: u32,
    base: &'a target::Target,
    current: Current<'a>,
}

impl<'a> State<'a> {
    pub fn new(limit: u32, base: &'a target::Target, path: &'a str) -> Result<Self, crate::Error> {
        let current = match borrowable_target(base, path) {
            Some(path) => Current::Borrowed(path),
            None => {
                let target = resolve(base, path)?;
                let relation = origin_relation(base, &target);
                Current::Owned {
                    visited: History::try_new(<NonZero<usize>>::MIN, target)?,
                    relation,
                }
            }
        };
        Ok(Self {
            limit,
            phase_limit: limit,
            base,
            current,
        })
    }

    pub fn origin(&self) -> &target::Target {
        match &self.current {
            Current::Borrowed(_) => self.base,
            Current::Owned { visited, .. } => visited.prior.first().unwrap_or(&visited.current),
        }
    }

    pub fn is_base(&self) -> bool {
        match &self.current {
            Current::Borrowed(_) => true,
            Current::Owned { relation, .. } => *relation == Relation::Same,
        }
    }

    fn has_prior(&self) -> bool {
        match &self.current {
            Current::Borrowed(_) => false,
            Current::Owned { visited, .. } => !visited.prior.is_empty(),
        }
    }

    fn advance(self, location: &str, changed: bool) -> Result<Self, crate::Error> {
        if self.limit == 0 {
            return Err(crate::Error::Redirect(Error::Limit));
        }
        let State {
            limit,
            phase_limit: phase,
            base,
            current,
        } = self;
        let (visited, relation) = match current {
            Current::Borrowed(path) => {
                let initial = resolve(base, path)?;
                let next = resolve_redirect(&initial, location)?;
                if !changed && initial.path_query() == next.path_query() {
                    return Err(crate::Error::Redirect(Error::Loop));
                }
                let mut visited = History::try_new(history_capacity(limit), initial)?;
                visited.try_push(next)?;
                (visited, Relation::Same)
            }
            Current::Owned {
                mut visited,
                relation,
            } => {
                let next = resolve_redirect(&visited.current, location)?;
                if !changed && visited.contains(&next, phase - limit) {
                    return Err(crate::Error::Redirect(Error::Loop));
                }
                visited.try_push(next)?;
                (visited, relation)
            }
        };
        let limit = limit - 1;
        Ok(Self {
            limit,
            phase_limit: if changed { limit } else { phase },
            base,
            current: Current::Owned { visited, relation },
        })
    }
}

impl Target for State<'_> {
    fn origin_form(&self) -> &str {
        match &self.current {
            Current::Borrowed(path) => path,
            Current::Owned { visited, .. } => visited.current.path_query(),
        }
    }
}

fn history_capacity(limit: u32) -> NonZero<usize> {
    <NonZero<usize>>::MIN.saturating_add(limit.min(16) as usize)
}

fn borrowable_target<'a>(base: &'a target::Target, target: &'a str) -> Option<&'a str> {
    let target = target.split_once('#').map_or(target, |split| split.0);
    if target.is_empty() {
        return Some(base.path_query());
    }
    let path = target.split_once('?').map_or(target, |split| split.0);
    if !path.starts_with('/') || path.starts_with("//") || has_dot_segment(path) {
        return None;
    }
    target::Origin::parse(target).is_ok().then_some(target)
}

fn has_dot_segment(path: &str) -> bool {
    path.split('/').any(|segment| matches!(segment, "." | ".."))
}
