use sark_http::field::metadata::lower;

use crate::{frame, qpack::decoder};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Failure {
    Pressure,
    Decode,
}

impl Failure {
    pub(super) const fn classify(error: &decoder::Error) -> Self {
        match error {
            decoder::Error::Allocation(_)
            | decoder::Error::FieldCapacity { .. }
            | decoder::Error::LiteralCapacity { .. }
            | decoder::Error::PoolExhausted(_)
            | decoder::Error::PoolEmpty
            | decoder::Error::Lower(
                lower::Error::Plain(_)
                | lower::Error::Layout(_)
                | lower::Error::Prefix(_, _)
                | lower::Error::Capacity { .. }
                | lower::Error::Scratch(_)
                | lower::Error::Write(_)
                | lower::Error::SinkProgress { .. },
            ) => Self::Pressure,
            _ => Self::Decode,
        }
    }

    pub(super) const fn is_pressure(self) -> bool {
        matches!(self, Self::Pressure)
    }

    pub(super) const fn code(self) -> frame::ErrorCode {
        match self {
            Self::Pressure => frame::ErrorCode::ExcessiveLoad,
            Self::Decode => frame::ErrorCode::QpackDecompressionFailed,
        }
    }
}
