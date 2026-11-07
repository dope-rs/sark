use o3::buffer::segments;
use sark_core::body::{self, credit};

use crate::frame::numbers;

#[derive(Debug)]
pub(crate) enum Error {
    Body(body::Error),
    Segments(segments::Error),
}

impl Error {
    pub(crate) const fn code(&self) -> numbers::ErrorCode {
        match self {
            Self::Body(
                body::Error::LengthMismatch { .. }
                | body::Error::InputIncomplete
                | body::Error::InputFinished,
            ) => numbers::ErrorCode::ProtocolError,
            Self::Body(
                body::Error::TooLarge { .. }
                | body::Error::Overflow { .. }
                | body::Error::Credit(
                    credit::Error::ByteLimit { .. } | credit::Error::ChunkLimit { .. },
                ),
            )
            | Self::Segments(
                segments::Error::SegmentCapacity(_)
                | segments::Error::ChainCapacity(_)
                | segments::Error::Retain(_),
            ) => numbers::ErrorCode::EnhanceYourCalm,
            Self::Body(
                body::Error::SegmentMismatch
                | body::Error::Credit(
                    credit::Error::ByteOverflow { .. }
                    | credit::Error::ChunkOverflow { .. }
                    | credit::Error::ByteUnderflow { .. }
                    | credit::Error::ChunkUnderflow { .. },
                ),
            )
            | Self::Segments(
                segments::Error::DifferentPool
                | segments::Error::Count(_)
                | segments::Error::Logical(_)
                | segments::Error::Resident(_)
                | segments::Error::Boundary(_),
            ) => numbers::ErrorCode::InternalError,
        }
    }
}
