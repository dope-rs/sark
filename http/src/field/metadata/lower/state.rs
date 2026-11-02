#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Ready,
    Scratch,
    Tagged,
    Stored,
    Pending,
}
