//! Static HTTP stream emission shared by protocol carriers.

use core::{num::NonZero, ops::ControlFlow};

use o3::buffer::segments;
use sark_http::field;

use crate::{life::session, manifold};

/// Whether one semantic stream event owns the peer's end marker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum Boundary {
    #[default]
    More,
    End,
}

const _: () = {
    assert!(core::mem::size_of::<Boundary>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Boundary>() == core::mem::align_of::<bool>());
};

/// Position of one field section in an HTTP message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Section {
    Initial,
    Trailing,
}

const _: () = {
    assert!(core::mem::size_of::<Section>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Section>() == core::mem::align_of::<bool>());
};

/// Carrier-packed stream identity and end marker.
pub trait Message: Copy {
    type Key: manifold::raw::Key;

    fn key(self) -> Self::Key;

    fn boundary(self) -> Boundary;
}

/// Affine retained DATA ownership supplied by one carrier.
pub trait Payload<C: session::Storage>: Sized {
    /// Admission owner released by `Drop` when its DATA bytes leave framing.
    type Permit;

    fn into_parts(self) -> (segments::Chain<C>, Self::Permit);
}

/// Result of emitting one complete head or trailer field section.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Control<B> {
    Written,
    /// Encoded once and retained by the carrier; retry with `Write::commit`.
    Prepared(B),
    /// Not admitted; a later call must provide the source again.
    Blocked(B),
}

/// Result of emitting a non-empty body prefix.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Data<B> {
    Written(NonZero<usize>),
    Blocked(B),
}

/// Static write access to one native HTTP stream.
pub trait Write<P> {
    type Block;
    type Error;

    fn headers<S: field::Source>(
        &mut self,
        source: S,
        boundary: Boundary,
    ) -> Result<Control<Self::Block>, Self::Error>;

    fn data(
        &mut self,
        prefix: &[u8],
        payload: &P,
        offset: usize,
        boundary: Boundary,
    ) -> Result<Data<Self::Block>, Self::Error>;

    /// Retries only this stream's retained field section, without evaluating a source.
    fn commit(&mut self) -> Result<ControlFlow<Self::Block>, Self::Error>;

    /// Discards only this stream's retained field section.
    fn discard(&mut self);
}
