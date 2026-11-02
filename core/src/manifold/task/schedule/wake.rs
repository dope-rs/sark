/// Exact reason a pending task cannot be woken.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Missing,
    Inactive,
}
