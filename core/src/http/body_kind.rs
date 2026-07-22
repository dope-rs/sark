#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ResponseKind {
    Inline,
    Static,
    Stream,
}
