use o3::mem::Rejected;
use sark_http::request::body::Source;

use crate::{body, body::admit, manifold::stream::Boundary};

/// Route-selected payload policy before carrier bounds are applied.
pub enum Plan {
    Known {
        max: usize,
        length: usize,
        retention: admit::Retention,
        stream: admit::Stream,
    },
    Unknown {
        max: usize,
        retention: admit::Retention,
        stream: admit::Stream,
    },
}

impl Plan {
    pub const fn new(
        mode: admit::Retention,
        max: usize,
        length: admit::Length,
        stream: admit::Stream,
    ) -> Self {
        match length {
            admit::Length::Known(length) => Self::Known {
                max,
                length,
                retention: mode,
                stream,
            },
            admit::Length::Unknown => Self::Unknown {
                max,
                retention: mode,
                stream,
            },
        }
    }

    /// Admits unfinished input while retaining its exact open phase.
    pub fn try_open(&self, limit: usize) -> Result<admit::Track<admit::Open>, body::Error> {
        let bound = match *self {
            Self::Known {
                max,
                length,
                retention: mode,
                stream,
            } => {
                let limit = limit.min(max);
                if length > limit {
                    return Err(body::Error::TooLarge {
                        actual: length,
                        limit,
                    });
                }
                admit::Bound {
                    kind: admit::Kind::Exact,
                    value: length,
                    retention: mode,
                    stream,
                    phase: admit::Open,
                }
            }
            Self::Unknown {
                max,
                retention: mode,
                stream,
            } => admit::Bound {
                kind: admit::Kind::Limit,
                value: limit.min(max),
                retention: mode,
                stream,
                phase: admit::Open,
            },
        };
        Ok(admit::Track::new(bound))
    }

    pub fn try_start(&self, limit: usize, boundary: Boundary) -> Result<admit::Start, body::Error> {
        let track = self.try_open(limit)?;
        Ok(match boundary {
            Boundary::End => admit::Start::Finished(track.try_complete()?),
            Boundary::More => admit::Start::Open(track),
        })
    }

    /// Admits an already complete logical payload.
    pub fn try_admit(
        &self,
        limit: usize,
        len: usize,
    ) -> Result<admit::Track<admit::Finished>, body::Error> {
        self.try_open(limit)?.try_admit(len)
    }

    pub fn try_finish<'d, S>(
        &self,
        limit: usize,
        len: usize,
        source: S,
    ) -> Result<body::Body<S>, admit::Error<S>>
    where
        S: Source<'d>,
    {
        let track = match self.try_admit(limit, len) {
            Ok(track) => track,
            Err(error) => {
                return Err(admit::Error::Admission(Rejected::new(source, error)));
            }
        };
        body::Body::try_new(track, source).map_err(admit::Error::Storage)
    }
}

const _: () = assert!(core::mem::size_of::<Plan>() == 3 * core::mem::size_of::<usize>());
