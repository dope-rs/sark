mod driver;

pub use driver::Driver;

/// Whether a completed data turn keeps its resident stream.
pub enum Settlement {
    Keep,
    Remove,
}
