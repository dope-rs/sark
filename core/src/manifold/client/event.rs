use std::fmt;

use crate::manifold::{client, stream};

/// Carrier failure class visible above a wire implementation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Blocked,
    Unavailable,
    Capacity,
    Protocol,
}

/// Classifies a carrier failure without consuming its exact cause.
pub trait Classify: fmt::Debug {
    fn kind(&self) -> Error;
}

/// Semantic reason for retiring one request stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reset {
    Cancelled,
    Refused,
    Internal,
    Other,
    /// Carrier reports exhausted bandwidth.
    ResourceExhausted,
    /// Carrier reports insufficient protocol security.
    InadequateSecurity,
}

/// Peer connection shutdown classification.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Shutdown {
    Clean,
    Failed,
}

const _: () = assert!(core::mem::size_of::<Shutdown>() == core::mem::size_of::<bool>());
const _: () = assert!(core::mem::align_of::<Shutdown>() == core::mem::align_of::<bool>());

/// One response-side event produced by an HTTP client carrier.
pub enum Response<C: client::Connection + ?Sized> {
    Head {
        message: C::Message,
        fields: C::Fields,
        selection: C::Selection,
        section: stream::Section,
    },
    Data {
        message: C::Message,
        data: C::Data,
    },
    Reset {
        stream: C::Key,
        reason: Reset,
    },
    GoAway {
        boundary: C::GoAway,
        shutdown: Shutdown,
    },
}
