//! Static HTTP client carrier boundary.

use o3::buffer::storage;
use sark_http::field;

use crate::{
    life::session,
    manifold::{self, stream},
};

pub mod domain;
pub mod event;
pub mod io;

/// One protocol-native HTTP client connection.
pub trait Connection {
    type Error: event::Classify;
    type Owner: session::Storage;
    type Key: manifold::raw::Key;
    type Block: Copy + Eq;
    type Message: stream::Message<Key = Self::Key>;
    type Fields;
    type Selection;
    type Data: stream::Payload<Self::Owner>;
    type GoAway;

    fn start<S: field::Source>(
        &mut self,
        fields: S,
        boundary: stream::Boundary,
    ) -> Result<Self::Key, Self::Error>;

    /// Sends from `prefix` followed by `body[offset..]`.
    /// `Written(n)` counts bytes accepted from that sequence and must not exceed
    /// its remaining length. Zero is valid, including an empty end boundary.
    fn send(
        &mut self,
        stream: Self::Key,
        prefix: &[u8],
        body: &[u8],
        offset: usize,
        boundary: stream::Boundary,
    ) -> Result<io::Outcome<Self::Block>, Self::Error>;

    /// The owned-body equivalent of `send`, with the same written-count contract.
    fn send_owned(
        &mut self,
        stream: Self::Key,
        prefix: &[u8],
        body: &storage::Shared,
        offset: usize,
        boundary: stream::Boundary,
    ) -> Result<io::Outcome<Self::Block>, Self::Error>;

    /// Schedules a reset, returning the original key if it is no longer active.
    fn reset(&mut self, stream: Self::Key, reason: event::Reset) -> Result<(), Self::Key>;

    fn cancel(&mut self, stream: Self::Key, reason: event::Reset);

    fn poll(&mut self) -> Option<event::Response<Self>>;

    fn is_accepted(&self, bound: &Self::GoAway, stream: Self::Key) -> bool;
}
