use core::num::NonZero;

use crate::payload::response;

/// Message framing values admitted before the immutable field section is published.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Framing {
    length: u64,
    status: Option<NonZero<u16>>,
    method: Option<response::Method>,
    present: bool,
}

impl Framing {
    pub(super) const fn new(
        length: u64,
        status: Option<NonZero<u16>>,
        method: Option<response::Method>,
        present: bool,
    ) -> Self {
        Self {
            length,
            status,
            method,
            present,
        }
    }

    pub const fn content_length(self) -> Option<u64> {
        if self.present {
            Some(self.length)
        } else {
            None
        }
    }

    pub const fn status(self) -> Option<NonZero<u16>> {
        self.status
    }

    pub const fn method(self) -> Option<response::Method> {
        self.method
    }

    /// Carries the admitted message framing into incremental content accounting.
    pub fn content(self, method: response::Method) -> Result<(Content, u64), ContentError> {
        let length = self.content_length();
        if let Some(status) = self.status {
            match response::Content::new(method, status.get()) {
                response::Content::Tunnel => return Ok((Content::Tunnel, 0)),
                response::Content::Forbidden => {
                    if let Some(length) = length {
                        return Err(ContentError::ForbiddenLength { length });
                    }
                    return Ok((Content::Empty, 0));
                }
                response::Content::Suppressed => return Ok((Content::Empty, 0)),
                response::Content::Empty => {
                    if let Some(length) = length.filter(|&length| length != 0) {
                        return Err(ContentError::NonemptyLength { length });
                    }
                    return Ok((Content::Exact, 0));
                }
                response::Content::Allowed => {}
            }
        } else if self.method == Some(response::Method::Connect) {
            return Ok((Content::Tunnel, 0));
        }
        Ok(match length {
            Some(length) => (Content::Exact, length),
            None => (Content::Unbounded, 0),
        })
    }
}

/// Content mode kept separately from the remaining count to preserve compact stream state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Content {
    #[default]
    Unbounded,
    Exact,
    Empty,
    Tunnel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContentError {
    ForbiddenLength { length: u64 },
    NonemptyLength { length: u64 },
    Exceeded { remaining: u64, actual: usize },
    Incomplete { remaining: u64 },
    Trailers,
}

impl Content {
    pub fn receive(
        self,
        remaining: u64,
        bytes: usize,
        finished: bool,
    ) -> Result<u64, ContentError> {
        let next = match self {
            Self::Unbounded | Self::Tunnel => return Ok(remaining),
            Self::Empty if bytes == 0 => return Ok(0),
            Self::Empty => {
                return Err(ContentError::Exceeded {
                    remaining: 0,
                    actual: bytes,
                });
            }
            Self::Exact => remaining
                .checked_sub(bytes as u64)
                .ok_or(ContentError::Exceeded {
                    remaining,
                    actual: bytes,
                })?,
        };
        if finished && next != 0 {
            return Err(ContentError::Incomplete { remaining: next });
        }
        Ok(next)
    }

    pub fn trailers(self, remaining: u64) -> Result<(), ContentError> {
        match self {
            Self::Unbounded => Ok(()),
            Self::Exact if remaining == 0 => Ok(()),
            Self::Exact => Err(ContentError::Incomplete { remaining }),
            Self::Empty | Self::Tunnel => Err(ContentError::Trailers),
        }
    }
}

const _: () = {
    assert!(core::mem::size_of::<Framing>() == 16);
    assert!(core::mem::size_of::<Option<Framing>>() == 16);
};
