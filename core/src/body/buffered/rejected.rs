use o3::mem;

use crate::body::{self, admit, buffered, credit};

/// A failed completion retaining its source, phase, and unreleased credit.
#[must_use]
#[derive(Debug)]
pub enum Rejected<S, T, O: credit::Origin = credit::raw::Stored> {
    Admission {
        state: buffered::State<S, O>,
        error: body::Error,
    },
    Storage {
        failure: mem::Rejected<(admit::Track<admit::Finished>, T), body::Error>,
        release: credit::Release<O>,
    },
    Credit {
        body: body::Body<T>,
        release: credit::Release<O>,
        error: body::Error,
    },
}

impl<S, T, O: credit::Origin> Rejected<S, T, O> {
    pub const fn error(&self) -> &body::Error {
        match self {
            Self::Admission { error, .. } | Self::Credit { error, .. } => error,
            Self::Storage { failure, .. } => failure.error(),
        }
    }

    /// Releases rejected input credit while retaining its owner for final policy.
    pub fn try_release<E: credit::Evidence>(
        mut self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: &mut E,
    ) -> Result<Self, mem::Rejected<Self, credit::Error>> {
        let amount = match &mut self {
            Self::Admission { state, .. } => &mut state.state.credit,
            Self::Storage { release, .. } | Self::Credit { release, .. } => &mut release.amount,
        };
        let freed = match ledger.amount().try_drain(amount) {
            Ok(change) => change.commit(),
            Err(error) => return Err(mem::Rejected::new(self, error)),
        };
        event.released(freed, credit::Disposition::Discarded);
        Ok(self)
    }
}
