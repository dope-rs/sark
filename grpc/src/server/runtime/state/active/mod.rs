mod entry;
mod error;
mod signals;

pub use entry::Entry;
pub use error::Error;
pub(in crate::server) use signals::Signals;

pub(in crate::server) enum Edge {
    None,
    Wake,
}
