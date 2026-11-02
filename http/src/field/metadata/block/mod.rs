use crate::field::metadata::map::record::Span;

pub mod buffer;
mod framing;
mod gate;
mod name;
mod outcome;
mod plan;
mod scan;
pub mod table;
mod value;

pub use framing::{Content, ContentError, Framing};
pub use gate::Gate;
pub use name::Name;
pub use outcome::Outcome;
pub use plan::Plan;
pub use scan::Scan;
pub use value::Value;

/// A classification bound to the immutable bytes that established it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Named<'a> {
    span: Span<'a>,
    proof: Name,
}

impl<'a> Named<'a> {
    pub fn from_span(span: Span<'a>) -> Self {
        Self::from_scanned(span, Name::from_span(span))
    }

    pub const fn from_slice(bytes: &'a [u8]) -> Self {
        Self::from_scanned(Span::from_slice(bytes), Name::classify(bytes))
    }

    pub(crate) const fn from_scanned(span: Span<'a>, proof: Name) -> Self {
        Self { span, proof }
    }

    pub const fn span(self) -> Span<'a> {
        self.span
    }

    pub const fn proof(self) -> Name {
        self.proof
    }
}

/// Value bytes with their retained admission verdict.
/// A rejected source field remains rejected when its value is reused.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Valued<'a> {
    span: Span<'a>,
    validation: Validation,
}

impl<'a> Valued<'a> {
    pub fn from_span(span: Span<'a>) -> Self {
        Self::from_scanned(span, Value::from_span(span).finish())
    }

    pub(crate) const fn from_scanned(span: Span<'a>, check: Validation) -> Self {
        Self {
            span,
            validation: check,
        }
    }

    pub const fn span(self) -> Span<'a> {
        self.span
    }

    pub const fn validation(self) -> Validation {
        self.validation
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Request,
    Response,
    Trailers,
    PushPromise,
}

/// Extended CONNECT capability advertised by the peer.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Connect {
    Disabled,
    Enabled,
}

/// Header validation rules selected by the wire protocol.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Version {
    Two,
    Three,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum Validation {
    Valid,
    Malformed,
    RejectPush,
}

/// The message phase established by a valid compressed response status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Response {
    Informational,
    Final,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Form {
    Path,
    Asterisk,
    Connect,
    ExtendedConnect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Authority {
    None,
    Pseudo,
    Host,
    Target,
}

const _: () = {
    assert!(core::mem::size_of::<Connect>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Connect>() == core::mem::align_of::<bool>());
    assert!(core::mem::size_of::<Version>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Version>() == core::mem::align_of::<bool>());
};
