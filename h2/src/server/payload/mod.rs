use sark_core::{
    body::{self, buffered, credit},
    decode::request,
};

use crate::frame::numbers;

mod buffer;
mod error;
pub(crate) mod raw;

pub(crate) use buffer::{Buffer, Push};
pub(crate) use error::Error;

pub(crate) fn discard<O: credit::Origin, E: credit::Evidence>(
    release: credit::Release<O>,
    ledger: &mut credit::raw::Ledger<'_, O>,
    mut evidence: E,
) -> Result<(), Error> {
    release
        .try_discard(ledger, &mut evidence)
        .abandon(evidence)
        .map_err(|error| Error::Body(body::Error::Credit(error)))
}

/// Releases the credit of rejected input and classifies its exact wire code.
pub(crate) fn release_rejected<K, P, S, T, E, O>(
    failure: request::Failure<K, P, buffered::Rejected<S, T, O>>,
    ledger: &mut credit::raw::Ledger<'_, O>,
    evidence: &mut E,
) -> numbers::ErrorCode
where
    E: credit::Evidence,
    O: credit::Origin,
{
    match failure.discard().try_release(ledger, evidence) {
        Ok(rejected) => Error::Body(*rejected.error()).code(),
        Err(_) => numbers::ErrorCode::InternalError,
    }
}
