#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::manifold::server) enum Work {
    Flush,
    Resume,
}
