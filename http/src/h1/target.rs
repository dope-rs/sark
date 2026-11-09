//! HTTP/1 request-target and authority admission.

use crate::{
    field::{self, scan::Match},
    request::{self, span},
    uri::{self, path},
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    InvalidHost,
    MissingHost,
    EmptyAuthority,
    InvalidTarget,
    Path(path::Error),
    Authority(uri::Error),
    Span(Operation, span::Error),
    Layout(request::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostError {
    Authority(uri::Error),
    Duplicate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Invalid;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Operation {
    Path,
    Query,
    Authority,
    PathShift,
    QueryShift,
    AuthorityShift,
}

impl Operation {
    fn check(self, start: usize, len: usize) -> Result<span::Span, Error> {
        match span::Span::checked(start, len) {
            Ok(span) => Ok(span),
            Err(cause) => Err(Error::Span(self, cause)),
        }
    }

    fn shift(self, span: span::Span, base: usize) -> Result<span::Span, Error> {
        match span.shifted(base) {
            Ok(span) => Ok(span),
            Err(cause) => Err(Error::Span(self, cause)),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Host {
    Empty,
    Authority,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
enum HostState {
    #[default]
    Missing,
    Empty,
    Authority,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Request {
    host: HostState,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Form {
    Origin,
    Absolute,
    Authority,
    Asterisk,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Route {
    Borrowed,
    Root,
    None,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Part {
    Missing,
    Present(span::Span),
}

/// Validated target ranges, relative until [`Target::shifted`] binds an owner.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Target {
    path: span::Span,
    query: Part,
    authority: Part,
    form: Form,
    route: Route,
}

impl Target {
    pub fn parse(method: &[u8], target: &[u8]) -> Result<Self, Error> {
        if method.is_empty()
            || !method.iter().copied().all(field::Error::is_name_byte)
            || target.is_empty()
        {
            return Err(Error::InvalidTarget);
        }
        if target == b"*" {
            return if method == b"OPTIONS" {
                Ok(Self {
                    path: span::Span::default(),
                    query: Part::Missing,
                    authority: Part::Missing,
                    form: Form::Asterisk,
                    route: Route::None,
                })
            } else {
                Err(Error::InvalidTarget)
            };
        }
        if method == b"CONNECT" {
            return if is_connect(target) {
                Ok(Self {
                    path: span::Span::default(),
                    query: Part::Missing,
                    authority: Part::Present(Operation::Authority.check(0, target.len())?),
                    form: Form::Authority,
                    route: Route::None,
                })
            } else {
                Err(Error::InvalidTarget)
            };
        }
        if target[0] == b'/' {
            let scan = path::Scan::new(target).complete().map_err(Error::Path)?;
            return origin(scan, 0);
        }

        let rest = scheme(target).ok_or(Error::InvalidTarget)?;
        let rest_at = target.len() - rest.len();
        let (auth, path_at, suffix) = if let Some(rest) = rest.strip_prefix(b"//") {
            let span_at = rest_at + 2;
            let (value, suffix) = match Match::find2(b'/', b'?', rest) {
                Some(found) => (found.prefix(), found.tail()),
                None => (rest, &[][..]),
            };
            let auth = if value.is_empty() {
                Part::Missing
            } else {
                uri::Authority::parse(value).map_err(Error::Authority)?;
                Part::Present(Operation::Authority.check(span_at, value.len())?)
            };
            (auth, span_at + value.len(), suffix)
        } else {
            (Part::Missing, rest_at, rest)
        };

        let scan = path::Scan::new(suffix).complete().map_err(Error::Path)?;
        let path_len = scan.path_end();
        let route = if path_len == 0 {
            Route::Root
        } else if suffix.starts_with(b"/") {
            Route::Borrowed
        } else {
            Route::None
        };
        Ok(Self {
            path: Operation::Path.check(path_at, path_len)?,
            query: match scan.query() {
                Some(start) => Part::Present(
                    Operation::Query.check(path_at + start.get(), scan.end() - start.get())?,
                ),
                None => Part::Missing,
            },
            authority: auth,
            form: Form::Absolute,
            route,
        })
    }

    pub(crate) fn parse_prefix(
        method: &[u8],
        bytes: &[u8],
        base: usize,
    ) -> Result<Option<(usize, Self)>, Error> {
        if bytes.first() == Some(&b'/') && method != b"CONNECT" {
            return match path::Scan::new(bytes).prefix() {
                path::Outcome::Complete(scan) => {
                    origin(scan, base).map(|target| Some((scan.end(), target)))
                }
                path::Outcome::Incomplete => Ok(None),
                path::Outcome::Invalid(error) => Err(Error::Path(error)),
            };
        }

        let Some(end) = Match::find2(b' ', b'\r', bytes) else {
            return Ok(None);
        };
        if end.start() == 0 || end.byte() == b'\r' {
            return Err(Error::InvalidTarget);
        }
        let target = Self::parse(method, end.prefix())?.shifted(base)?;
        Ok(Some((end.start(), target)))
    }

    pub fn shifted(self, base: usize) -> Result<Self, Error> {
        Ok(Self {
            path: Operation::PathShift.shift(self.path, base)?,
            query: match self.query {
                Part::Missing => Part::Missing,
                Part::Present(query) => Part::Present(Operation::QueryShift.shift(query, base)?),
            },
            authority: match self.authority {
                Part::Missing => Part::Missing,
                Part::Present(auth) => Part::Present(Operation::AuthorityShift.shift(auth, base)?),
            },
            ..self
        })
    }

    pub const fn form(self) -> Form {
        self.form
    }

    /// The source range backing the normalized route path. An empty range is
    /// intentional for an absolute URI whose omitted path routes as `/`.
    pub const fn path_span(self) -> span::Span {
        self.path
    }

    pub const fn query_span(self) -> Option<span::Span> {
        match self.query {
            Part::Missing => None,
            Part::Present(query) => Some(query),
        }
    }

    pub const fn authority(self) -> Option<span::Span> {
        match self.authority {
            Part::Missing => None,
            Part::Present(auth) => Some(auth),
        }
    }

    pub(crate) fn layout(
        self,
        target: span::Span,
        span: span::Span,
    ) -> Result<request::Layout, Error> {
        let form = match self.form {
            Form::Authority => request::Form::Authority,
            Form::Asterisk => request::Form::Asterisk,
            Form::Absolute if matches!(self.route, Route::Root) => request::Form::Root,
            Form::Origin | Form::Absolute => request::Form::Path,
        };
        let route = match self.route {
            Route::Borrowed | Route::Root => Some(self.path),
            Route::None => None,
        };
        let selected = (!matches!(form, request::Form::Authority)).then_some(span);
        request::Layout::try_new(form, target, selected, route).map_err(Error::Layout)
    }

    /// Projects the path used by an origin router without allocating or
    /// rewriting the original request-target.
    pub fn route_path(self, request: &[u8]) -> Option<&[u8]> {
        match self.route {
            Route::Borrowed => self.path.get(request),
            Route::Root => Some(b"/"),
            Route::None => match self.form {
                Form::Authority | Form::Asterisk => Some(b"*"),
                Form::Origin | Form::Absolute => None,
            },
        }
    }

    const fn source(self) -> Option<Source> {
        match self.form {
            Form::Absolute | Form::Authority => Some(match self.authority {
                Part::Present(auth) if !auth.is_empty() => Source::Target,
                Part::Missing | Part::Present(_) => Source::Empty,
            }),
            Form::Origin | Form::Asterisk => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Source {
    Empty,
    Host,
    Target,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Policy {
    AllowEmpty,
    Require,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Plan {
    target: Form,
    source: Source,
}

impl Request {
    pub fn try_host(self, value: &[u8]) -> Result<Self, HostError> {
        let host = Host::parse(value).map_err(HostError::Authority)?;
        self.try_push(host)
    }

    pub(crate) fn try_push(mut self, value: Host) -> Result<Self, HostError> {
        if !matches!(self.host, HostState::Missing) {
            return Err(HostError::Duplicate);
        }
        self.host = match value {
            Host::Empty => HostState::Empty,
            Host::Authority => HostState::Authority,
        };
        Ok(self)
    }

    pub const fn has_host(self) -> bool {
        !matches!(self.host, HostState::Missing)
    }

    pub fn finish(self, method: &[u8], target: &[u8], version: &[u8]) -> Result<Plan, Error> {
        let target = Target::parse(method, target)?;
        self.finish_plan(target, version)
    }

    /// Finalizes authority semantics from a request-target that was already
    /// validated by the request-line parser.
    pub fn finish_plan(self, target: Target, version: &[u8]) -> Result<Plan, Error> {
        if version == b"HTTP/1.1" && matches!(self.host, HostState::Missing) {
            return Err(Error::MissingHost);
        }

        let source = match target.source() {
            Some(source) => source,
            None => match self.host {
                HostState::Authority => Source::Host,
                HostState::Missing | HostState::Empty => Source::Empty,
            },
        };
        Ok(Plan {
            target: target.form(),
            source,
        })
    }
}

impl Plan {
    pub const fn target_form(self) -> Form {
        self.target
    }

    pub const fn source(self) -> Source {
        self.source
    }

    /// Applies the endpoint policy used by HTTP(S) origins, whose target URI
    /// cannot be reconstructed without a non-empty effective authority.
    pub const fn require(self) -> Result<Self, Error> {
        if matches!(self.source, Source::Empty) {
            Err(Error::EmptyAuthority)
        } else {
            Ok(self)
        }
    }

    /// Resolves the selected authority to a range in the retained request.
    /// Both inputs must use the same request-relative coordinate space.
    pub const fn span(self, target: Target, host: Option<span::Span>) -> Result<span::Span, Error> {
        match self.source {
            Source::Host => match host {
                Some(span) => Ok(span),
                None => Err(Error::InvalidHost),
            },
            Source::Target => match target.authority() {
                Some(span) => Ok(span),
                None => Err(Error::InvalidTarget),
            },
            Source::Empty => Err(Error::EmptyAuthority),
        }
    }
}

impl Host {
    pub fn parse(value: &[u8]) -> Result<Self, uri::Error> {
        let value = field::Error::trim_ows(value);
        if value.is_empty() {
            return Ok(Self::Empty);
        }
        uri::Authority::parse(value)?;
        Ok(Self::Authority)
    }
}

fn origin(scan: path::Plan, base: usize) -> Result<Target, Error> {
    let path_end = scan.path_end();
    Ok(Target {
        path: Operation::Path.check(base, path_end)?,
        query: match scan.query() {
            Some(start) => {
                Part::Present(Operation::Query.check(base + start.get(), scan.end() - start.get())?)
            }
            None => Part::Missing,
        },
        authority: Part::Missing,
        form: Form::Origin,
        route: Route::Borrowed,
    })
}

fn scheme(value: &[u8]) -> Option<&[u8]> {
    let mut bytes = value.iter();
    if !bytes.next()?.is_ascii_alphabetic() {
        return None;
    }
    while let Some(&byte) = bytes.next() {
        if byte == b':' {
            return Some(bytes.as_slice());
        }
        if !(byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.')) {
            return None;
        }
    }
    None
}

fn is_connect(value: &[u8]) -> bool {
    if value.first() == Some(&b'[') {
        let Some(close) = value.iter().position(|byte| *byte == b']') else {
            return false;
        };
        return close > 1
            && uri::Authority::is_ip(&value[1..close])
            && uri::Authority::is_port(&value[close + 1..]);
    }

    let Some(colon) = value.iter().rposition(|byte| *byte == b':') else {
        return false;
    };
    !value[..colon].contains(&b':')
        && uri::Authority::is_reg(&value[..colon])
        && uri::Authority::is_port(&value[colon..])
}

const _: () = assert!(core::mem::size_of::<Target>() <= 4 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Part>() == core::mem::size_of::<span::Span>());
const _: () = assert!(core::mem::size_of::<Request>() == 1);
const _: () = assert!(core::mem::size_of::<Plan>() <= 2);
const _: () = assert!(core::mem::size_of::<Operation>() == 1);
const _: () = assert!(core::mem::size_of::<Error>() == 5 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Option<Error>>() == core::mem::size_of::<Error>());
