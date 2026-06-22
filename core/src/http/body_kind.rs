#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ResponseKind {
    Inline,
    Static,
    Stream,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RequestKind {
    Inline,
    Spilled,
    Stream,
}
