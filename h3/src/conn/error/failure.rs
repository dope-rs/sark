use sark_http::field::metadata::lower;

use crate::qpack::decoder;

pub(super) const fn is_pressure(error: &decoder::Error) -> bool {
    matches!(
        error,
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
            )
    )
}
