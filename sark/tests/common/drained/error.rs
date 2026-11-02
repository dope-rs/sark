use sark_h2::{
    conn,
    frame::numbers::ParseError,
    stream::{Id, Key},
};

#[derive(Debug)]
pub enum Error {
    Transfer(conn::Error),
    Parse(ParseError),
    EmptyWrite,
    Owner {
        stream: Id,
        owner: Key,
        expected: usize,
        actual: usize,
    },
    Pointer {
        expected: usize,
        actual: usize,
    },
    Overflow {
        current: usize,
        additional: usize,
    },
}

impl Error {
    pub(super) fn add(current: usize, added: usize) -> Result<usize, Self> {
        current.checked_add(added).ok_or(Self::Overflow {
            current,
            additional: added,
        })
    }
}
