#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::server::task) enum Deadline {
    Ready,
    Expired,
    Wake,
    Critical,
}
