use core::{num::NonZero, ops};

use crate::field::metadata::block;

pub mod body;
pub mod raw;
pub mod span;

mod bytes;
mod id;
mod segments;
mod seq;
mod source;

pub use bytes::Bytes;
pub use id::Id;
pub use segments::Segments;
pub use seq::Seq;
pub use source::Source;
use span::Span;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Form {
    Path = 0,
    Root = 1,
    Asterisk = 2,
    Authority = 3,
    Extended = 4,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Span(span::Error),
    InvalidRange,
    OutOfBounds,
    MissingAuthority,
}

impl From<span::Error> for Error {
    fn from(error: span::Error) -> Self {
        Self::Span(error)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Target {
    packed_start_form_plus_one: NonZero<u32>,
    len: u32,
}

const TARGET_LANE: usize = 16;

fn target_lane_invalid<const N: usize>(lane: &[u8; N]) -> bool {
    let mut invalid = false;
    for &byte in lane {
        invalid |= (byte <= 0x20) | (byte == 0x7f);
    }
    invalid
}

impl Target {
    const FORM_BITS: u32 = 3;
    const FORM_MASK: u32 = (1 << Self::FORM_BITS) - 1;

    pub fn is_valid(bytes: &[u8]) -> bool {
        let (blocks, tail) = bytes.as_chunks::<{ TARGET_LANE * 4 }>();
        for block in blocks {
            if target_lane_invalid(block) {
                return false;
            }
        }
        let (lanes, tail) = tail.as_chunks::<TARGET_LANE>();
        for lane in lanes {
            if target_lane_invalid(lane) {
                return false;
            }
        }
        tail.iter().all(|&byte| byte > 0x20 && byte != 0x7f)
    }

    pub const fn from_span(form: Form, span: Span) -> Result<Self, Error> {
        let start = span.start();
        if start >= (1usize << (u32::BITS - Self::FORM_BITS)) {
            return Err(Error::OutOfBounds);
        }
        let packed = ((start as u32) << Self::FORM_BITS) | form as u32;
        let packed = NonZero::<u32>::MIN.saturating_add(packed);
        Ok(Self {
            packed_start_form_plus_one: packed,
            len: span.len() as u32,
        })
    }

    pub const fn form(self) -> Form {
        match (self.packed_start_form_plus_one.get() - 1) & Self::FORM_MASK {
            0 => Form::Path,
            1 => Form::Root,
            2 => Form::Asterisk,
            3 => Form::Authority,
            4..=u32::MAX => Form::Extended,
        }
    }

    pub const fn start(self) -> usize {
        ((self.packed_start_form_plus_one.get() - 1) >> Self::FORM_BITS) as usize
    }

    pub const fn len(self) -> usize {
        self.len as usize
    }

    pub const fn is_empty(self) -> bool {
        self.len == 0
    }

    pub const fn end(self) -> usize {
        self.start() + self.len()
    }

    pub fn get(self, input: &[u8]) -> Option<&[u8]> {
        input.get(self.start()..self.end())
    }
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    target: Target,
    authority: Option<Span>,
    route: Option<Span>,
}

/// A layout validated against the immutable owner carried with it.
pub struct Bound<O> {
    owner: O,
    layout: Layout,
}

impl<O: Source> Bound<O> {
    pub fn try_bind(owner: O, layout: Layout) -> Result<Self, Error> {
        if !layout.is_valid(owner.as_ref()) {
            return Err(Error::InvalidRange);
        }
        Ok(Self { owner, layout })
    }
}

impl<O> Bound<O> {
    pub fn into_parts(self) -> (O, Layout) {
        (self.owner, self.layout)
    }
}

impl Layout {
    /// The single checked construction path for a retained layout: the target
    /// span is packed for `form` and the companion spans are proved to sit
    /// inside that target before they are stored.
    pub fn try_new(
        form: Form,
        target: Span,
        peer: Option<Span>,
        route: Option<Span>,
    ) -> Result<Self, Error> {
        let layout = Self::new(Target::from_span(form, target)?, peer, route);
        if layout.is_shaped() {
            Ok(layout)
        } else {
            Err(Error::InvalidRange)
        }
    }

    /// Stores pre-packed parts without re-checking the shape they must share.
    ///
    /// Binding must prove the shape, ranges and bytes for the exact owner,
    /// either with [`Self::is_valid`] or by preserving parser admission.
    pub const fn new(target: Target, peer: Option<Span>, route: Option<Span>) -> Self {
        Self {
            target,
            authority: peer,
            route,
        }
    }

    fn from_kept(
        owner: &[u8],
        path: Option<ops::Range<usize>>,
        path_len: Option<usize>,
        pseudo: Option<ops::Range<usize>>,
        host: Option<ops::Range<usize>>,
        plan: block::Plan,
    ) -> Result<Self, Error> {
        let [path, pseudo, host] = checked_spans(owner, path, pseudo, host)?;
        let layout = Self::from_spans(path, path_len, pseudo, host, plan)?;
        if layout.checked(owner).is_some() {
            Ok(layout)
        } else {
            Err(Error::InvalidRange)
        }
    }

    fn from_spans(
        path: Option<Span>,
        path_len: Option<usize>,
        pseudo: Option<Span>,
        host: Option<Span>,
        plan: block::Plan,
    ) -> Result<Self, Error> {
        let selected = match plan.authority() {
            block::Authority::None => None,
            block::Authority::Pseudo | block::Authority::Target => pseudo,
            block::Authority::Host => host,
        };
        let (target, peer, route) = match plan.form() {
            block::Form::Path | block::Form::ExtendedConnect => {
                let target = path.ok_or(Error::InvalidRange)?;
                let len = path_len.ok_or(Error::InvalidRange)?;
                let route = Span::checked(target.start(), len).map_err(Error::Span)?;
                (target, selected, Some(route))
            }
            block::Form::Asterisk => (path.ok_or(Error::InvalidRange)?, selected, None),
            block::Form::Connect => (selected.ok_or(Error::MissingAuthority)?, None, None),
        };
        let form = match plan.form() {
            block::Form::Path => Form::Path,
            block::Form::Asterisk => Form::Asterisk,
            block::Form::Connect => Form::Authority,
            block::Form::ExtendedConnect => Form::Extended,
        };
        let target = Target::from_span(form, target)?;
        Ok(Self::new(target, peer, route))
    }

    pub const fn target(self) -> Target {
        self.target
    }

    pub const fn authority(self) -> Option<Span> {
        if matches!(self.target.form(), Form::Authority) {
            Some(Span::from_target(self.target))
        } else {
            self.authority
        }
    }

    pub const fn route(self) -> Option<Span> {
        self.route
    }

    pub fn route_path(self, owner: &[u8]) -> Option<&[u8]> {
        match self.target.form() {
            Form::Root => Some(b"/"),
            Form::Asterisk | Form::Authority => Some(b"*"),
            Form::Path | Form::Extended => self.route?.get(owner),
        }
    }

    pub fn authority_in(self, owner: &[u8]) -> Option<&[u8]> {
        self.authority()?.get(owner)
    }

    pub const fn require(self) -> Result<Self, Error> {
        if matches!(self.target.form(), Form::Authority) || self.authority.is_some() {
            Ok(self)
        } else {
            Err(Error::MissingAuthority)
        }
    }

    /// The owner-independent structure every retained layout shares.
    const fn is_shaped(self) -> bool {
        match self.target.form() {
            Form::Path | Form::Extended => match self.route {
                Some(route) => {
                    route.start() >= self.target.start() && route.end() <= self.target.end()
                }
                None => false,
            },
            Form::Root => match self.route {
                Some(route) => {
                    route.is_empty()
                        && route.start() >= self.target.start()
                        && route.start() <= self.target.end()
                }
                None => false,
            },
            Form::Asterisk | Form::Authority => self.route.is_none(),
        }
    }

    pub fn is_valid(self, owner: &[u8]) -> bool {
        self.checked(owner).is_some_and(Target::is_valid)
    }

    fn checked(self, owner: &[u8]) -> Option<&[u8]> {
        if !self.is_shaped() || self.authority.is_some_and(|span| span.end() > owner.len()) {
            return None;
        }
        let target = self.target.get(owner)?;
        let shaped = match self.target.form() {
            Form::Path | Form::Extended => self.route.is_some_and(|route| {
                route.end() == self.target.end() || owner.get(route.end()) == Some(&b'?')
            }),
            Form::Root => self.route.is_some_and(|route| {
                route.start() == self.target.end() || owner.get(route.start()) == Some(&b'?')
            }),
            Form::Asterisk => target == b"*",
            Form::Authority => true,
        };
        shaped.then_some(target)
    }
}

fn checked_span(range: Option<ops::Range<usize>>) -> Result<Option<Span>, Error> {
    range.map(Span::from_range).transpose().map_err(Error::Span)
}

fn checked_spans(
    owner: &[u8],
    path: Option<ops::Range<usize>>,
    pseudo: Option<ops::Range<usize>>,
    host: Option<ops::Range<usize>>,
) -> Result<[Option<Span>; 3], Error> {
    let path = checked_span(path)?;
    let pseudo = checked_span(pseudo)?;
    let host = checked_span(host)?;
    if path.is_some_and(|span| span.end() > owner.len())
        || pseudo.is_some_and(|span| span.end() > owner.len())
        || host.is_some_and(|span| span.end() > owner.len())
    {
        return Err(Error::InvalidRange);
    }
    Ok([path, pseudo, host])
}

const _: () = assert!(core::mem::size_of::<Target>() == 8);
const _: () = assert!(core::mem::size_of::<Option<Target>>() == 8);
const _: () = assert!(core::mem::size_of::<Layout>() == 24);
