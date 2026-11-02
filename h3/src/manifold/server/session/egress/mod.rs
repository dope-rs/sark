mod error;

pub(in crate::manifold::server) use error::Error;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::manifold::server) enum Work {
    Flush,
    Resume,
}
