//! Static HTTP server carrier boundary.

use crate::{
    life::session,
    manifold::{self, client::event::Reset, schedule, stream},
};

pub mod app;
pub mod drain;
mod event;
pub mod timing;

pub use event::Event;

/// One protocol-native HTTP server connection.
pub trait Connection {
    type Owner: session::Storage;
    type Key: manifold::raw::Key;
    type Block: schedule::Block;
    type Message: stream::Message<Key = Self::Key>;
    type Fields;
    type Selection;
    /// Carrier proof that an emitted field section passed protocol admission.
    type Control;
    type Permit;
    type Data: stream::Payload<Self::Owner, Permit = Self::Permit>;
    type Retired;
    type Output;
    type Error;
    type Writer<'a>: stream::Write<Self::Output, Block = Self::Block, Error = Self::Error>
    where
        Self: 'a;
    type Ready<'a>: drain::Ready<Item = Event<Self>>
    where
        Self: 'a;

    fn has_event(&mut self) -> bool;

    fn ready(&mut self) -> Option<Self::Ready<'_>>;

    fn poll_wake(&mut self) -> Option<schedule::Wake<Self::Key>>;

    fn writer(&mut self, stream: Self::Key) -> Self::Writer<'_>;

    fn reset(&mut self, stream: Self::Key, reason: Reset);

    fn fail(&mut self);
}
