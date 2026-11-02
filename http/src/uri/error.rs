#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Empty,
    InvalidHost,
    InvalidPort,
    InvalidScheme,
}
