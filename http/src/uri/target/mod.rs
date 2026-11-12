//! Lossless absolute-URI and client-target proofs.

use std::{mem, num, str};

use o3::alloc::{self, StrExt as _, array};

use crate::uri::{self, path};

mod plan;
pub use plan::Plan;
mod raw;
mod relative;
pub use relative::Relative;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Colon,
    Scheme(uri::Error),
    Separator { colon: usize, len: usize },
    UserInfo,
    Authority(uri::Error),
    Start { byte: Option<u8> },
    Origin(path::Error),
    Length,
    Index { value: usize },
    Allocation(alloc::Exhausted),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Absolute<'a> {
    scheme: &'a str,
    parsed: uri::Authority<'a>,
    origin: Origin<'a>,
}

impl<'a> Absolute<'a> {
    pub fn parse(value: &'a str) -> Result<Self, Error> {
        let bytes = value.as_bytes();
        let colon = bytes
            .iter()
            .position(|byte| *byte == b':')
            .ok_or(Error::Colon)?;
        // SAFETY: the located ASCII ':' ends this UTF-8 prefix inside the same input.
        let scheme = unsafe { raw::slice(value, 0..colon) };
        uri::Scheme::validate(scheme.as_bytes()).map_err(Error::Scheme)?;
        let start = colon.checked_add(3).ok_or(Error::Length)?;
        if bytes.get(colon..start) != Some(b"://") {
            return Err(Error::Separator {
                colon,
                len: bytes.len(),
            });
        }
        let rest = bytes.get(start..).ok_or(Error::Length)?;
        let len = match rest
            .iter()
            .position(|byte| matches!(*byte, b'/' | b'?' | b'#'))
        {
            Some(len) => len,
            None => rest.len(),
        };
        let end = start.checked_add(len).ok_or(Error::Length)?;
        let raw = value.get(start..end).ok_or(Error::Length)?;
        let parsed = authority(raw)?;
        let origin = Origin::from_uri(value.get(end..).ok_or(Error::Length)?)?;
        Ok(Self {
            scheme,
            parsed,
            origin,
        })
    }

    pub const fn scheme(self) -> &'a str {
        self.scheme
    }

    pub const fn authority(self) -> uri::Authority<'a> {
        self.parsed
    }

    pub const fn origin(self) -> Origin<'a> {
        self.origin
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Origin<'a> {
    value: &'a str,
    path_end: u32,
    slash: Slash,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Slash {
    Present,
    Insert,
}

impl<'a> Origin<'a> {
    pub const ROOT: Self = Self {
        value: "/",
        path_end: 1,
        slash: Slash::Present,
    };

    pub fn parse(value: &'a str) -> Result<Self, Error> {
        if !value.starts_with('/') {
            return Err(Error::Start {
                byte: value.as_bytes().first().copied(),
            });
        }
        Self::validated(value, Slash::Present)
    }

    fn from_uri(value: &'a str) -> Result<Self, Error> {
        match value.as_bytes().first() {
            Some(b'/') => Self::validated(value, Slash::Present),
            Some(b'?') => Self::validated(value, Slash::Insert),
            Some(byte) => Err(Error::Start { byte: Some(*byte) }),
            None => Self::validated(value, Slash::Insert),
        }
    }

    fn validated(value: &'a str, slash: Slash) -> Result<Self, Error> {
        let plan = path::Scan::new(value.as_bytes())
            .complete()
            .map_err(Error::Origin)?;
        Ok(Self {
            value,
            path_end: narrow(plan.path_end())?,
            slash,
        })
    }

    fn len(self) -> Result<usize, Error> {
        let prefix = match self.slash {
            Slash::Present => 0,
            Slash::Insert => 1,
        };
        self.value.len().checked_add(prefix).ok_or(Error::Length)
    }

    fn append_to(self, output: &mut String) {
        match self.slash {
            Slash::Present => {}
            Slash::Insert => output.push('/'),
        }
        output.push_str(self.value);
    }

    fn path_end(self) -> usize {
        self.path_end as usize + usize::from(self.slash == Slash::Insert)
    }
}

/// An owned URI target with one admitted authority and origin form.
///
/// Component offsets retain the parser's bounds and grammar proofs. [`Self::try_clone`]
/// copies the owned bytes; targets never share reference-counted storage.
#[derive(Debug)]
pub struct Target {
    value: Box<str>,
    scheme_end: Option<num::NonZero<u32>>,
    host_end: u32,
    authority_end: u32,
    path_end: u32,
}

impl Target {
    pub fn parse(value: &str) -> Result<Self, Error> {
        let parsed = Absolute::parse(value)?;
        let start = parsed.scheme().len() + 3;
        let end = value.len() - parsed.origin().value.len();
        // SAFETY: parsing retained these authority boundaries in this exact immutable input.
        let raw = unsafe { raw::slice(value, start..end) };
        pack(
            Some(parsed.scheme()),
            raw,
            parsed.authority(),
            parsed.origin(),
        )
    }

    pub fn new(raw: &str, origin: Origin<'_>) -> Result<Self, Error> {
        let parsed = authority(raw)?;
        pack(None, raw, parsed, origin)
    }

    pub fn from_host(
        host: &str,
        port: Option<num::NonZero<u16>>,
        origin: Origin<'_>,
    ) -> Result<Self, Error> {
        let brackets = host.contains(':') && !host.starts_with('[');
        let host_end = if brackets {
            if !uri::Authority::is_ip(host.as_bytes()) {
                return Err(Error::Authority(uri::Error::InvalidHost));
            }
            host.len().checked_add(2).ok_or(Error::Length)?
        } else {
            if host.is_empty() && port.is_some() {
                return Err(Error::Authority(uri::Error::InvalidHost));
            }
            let parsed = uri::Authority::parse(host.as_bytes()).map_err(Error::Authority)?;
            if port.is_some() && parsed.port().is_some() {
                return Err(Error::Authority(uri::Error::InvalidPort));
            }
            parsed.host().len()
        };
        let mut digits = [mem::MaybeUninit::uninit(); 20];
        let port = port.map(|port| raw::decimal(port, &mut digits));
        let port_len = port.map_or(0, |port| port.len() + 1);
        let end = host
            .len()
            .checked_add(usize::from(brackets) * 2)
            .and_then(|len| len.checked_add(port_len))
            .ok_or(Error::Length)?;
        let capacity = end.checked_add(origin.len()?).ok_or(Error::Length)?;
        let path_end = end.checked_add(origin.path_end()).ok_or(Error::Length)?;
        let bounds = plan::Bounds::new(0, host_end, end, path_end)?;
        let mut value = buffer(capacity)?;
        if brackets {
            value.push('[');
        }
        value.push_str(host);
        if brackets {
            value.push(']');
        }
        if let Some(port) = port {
            value.push(':');
            value.push_str(port);
        }
        origin.append_to(&mut value);
        // SAFETY: every measured host, port, and origin byte filled the exact capacity.
        Ok(bounds.build(unsafe { raw::boxed(value) }))
    }

    pub fn scheme_str(&self) -> Option<&str> {
        self.scheme_end.map(|end| {
            // SAFETY: admission retains the ASCII scheme prefix in this immutable owner.
            unsafe { raw::slice(&self.value, 0..end.get() as usize) }
        })
    }

    /// Copies the admitted bytes and offsets without parsing again.
    pub fn try_clone(&self) -> Result<Self, alloc::Exhausted> {
        let value = self.value.try_to_owned()?;
        Ok(Self {
            // SAFETY: StrExt copies this exact UTF-8 allocation with length equal to capacity.
            value: unsafe { raw::boxed(value) },
            scheme_end: self.scheme_end,
            host_end: self.host_end,
            authority_end: self.authority_end,
            path_end: self.path_end,
        })
    }

    pub fn authority(&self) -> &str {
        // SAFETY: construction retains these authority boundaries in this immutable owner.
        unsafe { raw::slice(&self.value, self.start()..self.authority_end as usize) }
    }

    /// The host spelling in the authority, including IPv6 brackets.
    pub fn host_raw(&self) -> &str {
        // SAFETY: construction retains this authority host range on ASCII boundaries.
        unsafe { raw::slice(&self.value, self.start()..self.host_end as usize) }
    }

    pub fn host(&self) -> &str {
        let host = self.host_raw();
        host.strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host)
    }

    pub fn path(&self) -> &str {
        let range = self.authority_end as usize..self.path_end as usize;
        // SAFETY: admission retains the complete ASCII path between these boundaries.
        unsafe { raw::slice(&self.value, range) }
    }

    pub fn path_query(&self) -> &str {
        // SAFETY: the admitted ASCII authority ends at this immutable owner's origin.
        unsafe { raw::slice(&self.value, self.authority_end as usize..self.value.len()) }
    }

    pub fn query(&self) -> Option<&str> {
        let end = self.path_end as usize;
        (end != self.value.len()).then(|| {
            // SAFETY: an admitted nonterminal path boundary contains its ASCII query separator.
            unsafe { raw::slice(&self.value, end + 1..self.value.len()) }
        })
    }

    pub fn retarget(self, raw: &str) -> Result<Self, Error> {
        let parsed = authority(raw)?;
        pack(self.scheme_str(), raw, parsed, Origin::from(&self))
    }

    pub fn with_origin(&self, origin: Origin<'_>) -> Result<Self, Error> {
        pack(self.scheme_str(), self.authority(), self.parts(), origin)
    }

    /// Removes dot segments while retaining the admitted component syntax.
    pub fn normalize(self) -> Result<Self, Error> {
        let relative = Relative::from_target(&self);
        if !relative.has_dots() {
            return Ok(self);
        }
        relative.materialize(&self)
    }

    fn start(&self) -> usize {
        match self.scheme_end {
            Some(end) => end.get() as usize + 3,
            None => 0,
        }
    }

    /// Borrows the authority components proved when this target was admitted.
    pub fn parts(&self) -> uri::Authority<'_> {
        let host = self.host_raw().as_bytes();
        let port = (self.host_end != self.authority_end).then(|| {
            let range = self.host_end as usize + 1..self.authority_end as usize;
            // SAFETY: admission places ':' after the host and proves this ASCII port range.
            unsafe { raw::slice(&self.value, range) }.as_bytes()
        });
        uri::Authority::new(host, port)
    }
}

impl<'a> From<&'a Target> for Origin<'a> {
    fn from(target: &'a Target) -> Self {
        Self {
            value: target.path_query(),
            path_end: target.path_end - target.authority_end,
            slash: Slash::Present,
        }
    }
}

impl AsRef<str> for Target {
    fn as_ref(&self) -> &str {
        &self.value
    }
}

impl str::FromStr for Target {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !value.contains("://") {
            return Self::new(value, Origin::ROOT);
        }
        Self::parse(value)
    }
}

impl PartialEq for Target {
    fn eq(&self, other: &Self) -> bool {
        let scheme = match (self.scheme_str(), other.scheme_str()) {
            (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
            (None, None) => true,
            _ => false,
        };
        scheme
            && self.authority().eq_ignore_ascii_case(other.authority())
            && self.path_query() == other.path_query()
    }
}

impl Eq for Target {}

fn authority(raw: &str) -> Result<uri::Authority<'_>, Error> {
    if raw.as_bytes().contains(&b'@') {
        return Err(Error::UserInfo);
    }
    uri::Authority::parse(raw.as_bytes()).map_err(Error::Authority)
}

fn pack(
    scheme: Option<&str>,
    raw: &str,
    parsed: uri::Authority<'_>,
    origin: Origin<'_>,
) -> Result<Target, Error> {
    let start = match scheme {
        Some(scheme) => scheme.len().checked_add(3).ok_or(Error::Length)?,
        None => 0,
    };
    let end = start.checked_add(raw.len()).ok_or(Error::Length)?;
    let capacity = end.checked_add(origin.len()?).ok_or(Error::Length)?;
    let host_end = start + parsed.host().len();
    let path_end = end.checked_add(origin.path_end()).ok_or(Error::Length)?;
    let bounds = plan::Bounds::new(scheme.map_or(0, str::len), host_end, end, path_end)?;
    let mut value = buffer(capacity)?;
    if let Some(scheme) = scheme {
        value.push_str(scheme);
        value.push_str("://");
    }
    value.push_str(raw);
    origin.append_to(&mut value);
    // SAFETY: the scheme, authority, and origin filled the exact measured capacity.
    Ok(bounds.build(unsafe { raw::boxed(value) }))
}

fn narrow(value: usize) -> Result<u32, Error> {
    if value > u32::MAX as usize {
        return Err(Error::Index { value });
    }
    Ok(value as u32)
}

fn buffer(capacity: usize) -> Result<String, Error> {
    array::Plan::try_new(capacity)
        .map_err(|_| Error::Length)?
        .try_string()
        .map_err(Error::Allocation)
}

const _: () = assert!(core::mem::size_of::<Absolute<'_>>() <= 9 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Slash>() == core::mem::size_of::<bool>());
const _: () = assert!(core::mem::size_of::<Origin<'_>>() <= 3 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Target>() <= 4 * core::mem::size_of::<usize>());
