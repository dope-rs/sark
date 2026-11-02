use dope_quic::{
    conn::stream::resident,
    wire,
    wire::{stream::Id, varint},
};

use crate::frame;

/// H3 wire identity optionally fused with a transport-resident generation.
/// The key is a routing capability; its transport owns the stream lifetime.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Target {
    stream: wire::Packed,
    resident: Option<resident::Key>,
}

impl Target {
    pub const fn new(id: Id) -> Self {
        Self {
            stream: wire::Packed::new(id, wire::Mark::Clear),
            resident: None,
        }
    }

    pub(crate) const fn resident(id: Id, key: resident::Key) -> Self {
        Self {
            stream: wire::Packed::new(id, wire::Mark::Clear),
            resident: Some(key),
        }
    }

    pub const fn stream_id(self) -> Id {
        self.stream.id()
    }

    pub const fn key(self) -> Option<resident::Key> {
        self.resident
    }
}

impl From<Id> for Target {
    fn from(id: Id) -> Self {
        Self::new(id)
    }
}

/// An abandoned response's routing identity, retained for its original driver.
/// It cannot be reused as authority to send on another connection.
#[repr(transparent)]
pub struct Cancellation(Target);

impl Cancellation {
    pub(crate) const fn new(target: Target) -> Self {
        Self(target)
    }

    pub const fn stream_id(&self) -> Id {
        self.0.stream_id()
    }

    pub(crate) const fn into_target(self) -> Target {
        self.0
    }
}

/// Owner-tagged H3 target that may outlive one QUIC connection access.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Owned {
    origin: resident::Origin,
    local: resident::Local,
}

impl Owned {
    pub(crate) const fn new(origin: resident::Origin, local: resident::Local) -> Self {
        Self { origin, local }
    }

    pub const fn stream_id(self) -> Id {
        self.local.id()
    }

    /// Returns the QUIC connection identity that owns this stream.
    pub const fn origin(self) -> resident::Origin {
        self.origin
    }

    /// Returns the owner-bound local QUIC stream handle.
    pub const fn local(self) -> resident::Local {
        self.local
    }

    pub(crate) fn try_target(self, origin: Option<resident::Origin>) -> Result<Target, Mismatch> {
        match origin {
            None => Err(Mismatch::Unbound {
                expected: self.origin,
            }),
            Some(found) if found != self.origin => Err(Mismatch::Foreign {
                expected: self.origin,
                found,
            }),
            Some(_) => Ok(Target::resident(self.local.id(), self.local.key())),
        }
    }
}

/// Exact connection-origin mismatch for an owner-tagged stream target.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mismatch {
    Unbound {
        expected: resident::Origin,
    },
    Foreign {
        expected: resident::Origin,
        found: resident::Origin,
    },
}

impl Mismatch {
    pub const fn expected(self) -> resident::Origin {
        match self {
            Self::Unbound { expected } | Self::Foreign { expected, .. } => expected,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum UniType {
    Control,
    Push,
    QpackEncoder,
    QpackDecoder,
    Unknown(u64),
}

impl UniType {
    pub const fn from_wire(value: varint::VarInt) -> Self {
        match value {
            frame::STREAM_TYPE_CONTROL => Self::Control,
            frame::STREAM_TYPE_PUSH => Self::Push,
            frame::STREAM_TYPE_QPACK_ENCODER => Self::QpackEncoder,
            frame::STREAM_TYPE_QPACK_DECODER => Self::QpackDecoder,
            value => Self::Unknown(value.get()),
        }
    }

    pub const fn is_critical(self) -> bool {
        matches!(
            self,
            Self::Control | Self::QpackEncoder | Self::QpackDecoder
        )
    }
}

const _: () = {
    assert!(core::mem::size_of::<Target>() == 2 * core::mem::size_of::<u64>());
    assert!(core::mem::size_of::<Option<Target>>() == core::mem::size_of::<Target>());
    assert!(core::mem::size_of::<Owned>() == 3 * core::mem::size_of::<u64>());
    assert!(core::mem::size_of::<Option<Owned>>() == core::mem::size_of::<Owned>());
    assert!(core::mem::size_of::<Mismatch>() == 2 * core::mem::size_of::<u64>());
};
