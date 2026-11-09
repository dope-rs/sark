mod scoped;
mod vacant;

pub use scoped::Scoped;
pub(super) use vacant::Vacant;

/// Lifetime-independent HTTP body preserving its physical segments.
pub type Retained = o3::buffer::segments::Shape<o3::buffer::storage::Shared>;

const _: () = assert!(std::mem::size_of::<Retained>() <= 4 * std::mem::size_of::<usize>());
