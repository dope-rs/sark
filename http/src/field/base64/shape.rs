use crate::field::base64::Error;

#[derive(Clone, Copy)]
pub(super) struct Shape {
    groups: usize,
    tail: Tail,
}

#[derive(Clone, Copy)]
pub(super) enum Tail {
    None,
    Two,
    Three,
}

impl Shape {
    pub(super) fn new(bytes: &[u8]) -> Result<Self, Error> {
        let len = bytes.len();
        if bytes.ends_with(b"===") {
            return Err(Error::Padding {
                index: len - 3,
                byte: b'=',
            });
        }
        if bytes.ends_with(b"==") {
            return if len.is_multiple_of(4) {
                Ok(Self {
                    groups: (len - 2) / 4,
                    tail: Tail::Two,
                })
            } else {
                Err(Error::Length { actual: len })
            };
        }
        if bytes.ends_with(b"=") {
            return if len.is_multiple_of(4) {
                Ok(Self {
                    groups: (len - 1) / 4,
                    tail: Tail::Three,
                })
            } else {
                Err(Error::Length { actual: len })
            };
        }
        let groups = len / 4;
        match len % 4 {
            0 => Ok(Self {
                groups,
                tail: Tail::None,
            }),
            1 => Err(Error::Length { actual: len }),
            2 => Ok(Self {
                groups,
                tail: Tail::Two,
            }),
            3 => Ok(Self {
                groups,
                tail: Tail::Three,
            }),
            4.. => Err(Error::Length { actual: len }),
        }
    }

    pub(super) const fn full(groups: usize) -> Self {
        Self {
            groups,
            tail: Tail::None,
        }
    }

    pub(super) const fn groups(self) -> usize {
        self.groups
    }

    pub(super) const fn symbols(self) -> usize {
        self.groups * 4
            + match self.tail {
                Tail::None => 0,
                Tail::Two => 2,
                Tail::Three => 3,
            }
    }

    pub(super) const fn decoded(self) -> usize {
        self.groups * 3
            + match self.tail {
                Tail::None => 0,
                Tail::Two => 1,
                Tail::Three => 2,
            }
    }

    pub(super) const fn tail(self) -> Tail {
        self.tail
    }
}

const _: () = assert!(core::mem::size_of::<Shape>() == 2 * core::mem::size_of::<usize>());
