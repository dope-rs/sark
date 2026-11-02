#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Cause {
    Borrowed,
    Inline,
    Compressed,
    Framed,
    Cached,
}
