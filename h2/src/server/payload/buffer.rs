use o3::buffer::{resident, segments};
use sark_core::{
    body::{
        self,
        admit::{self, Start},
        buffered, credit,
        stream::Stream,
    },
    manifold::stream::Boundary,
};
use sark_http::payload::owned;

use crate::{conn::event, server::payload};

type Streaming<'d, B, O> = Result<
    Stream<'d, B, O>,
    (
        Stream<'d, B, O>,
        buffered::Transition<<B as body::Push<'d>>::Error>,
    ),
>;

/// H2 request-body accounting state and DATA owners, moved as one capability.
#[repr(transparent)]
pub(crate) struct Buffer<'d, O: credit::Origin = credit::raw::Stored> {
    pub(super) inner: buffered::State<segments::Chain<resident::Shared<'d>>, O>,
}

/// Exact DATA ownership after one H2 request-buffer transition.
#[must_use = "a rejected DATA owner must be handled"]
pub(crate) enum Push<D> {
    Accepted,
    Rejected { input: D, error: payload::Error },
}

impl<'d> Buffer<'d> {
    pub(crate) fn start(start: Start) -> Self {
        let inner = match start {
            Start::Open(open) => buffered::State::new(open),
            Start::Finished(finished) => buffered::State::finished(finished),
        };
        Self { inner }
    }
}

impl<'d, O: credit::Origin> Buffer<'d, O> {
    pub(crate) const fn max_body(&self) -> usize {
        self.inner.max_body()
    }

    pub(crate) const fn stream(&self) -> admit::Stream {
        self.inner.stream()
    }

    pub(crate) const fn len(&self) -> usize {
        self.inner.len()
    }

    pub(crate) fn discard<E: credit::Evidence>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        evidence: E,
    ) -> Result<(), payload::Error> {
        payload::discard(self.inner.into_release(), ledger, evidence)
    }
}

impl<'d, O: credit::Origin> Buffer<'d, O> {
    pub(crate) fn try_stream<B>(self, build: B, boundary: Boundary) -> Streaming<'d, B, O>
    where
        B: body::Push<'d>,
    {
        self.inner
            .try_stream(build, boundary, owned::Segments::from)
            .map_err(|failure| failure.into_parts())
    }

    pub(crate) fn try_finish<E: credit::Evidence>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        mut evidence: E,
    ) -> Result<
        body::Body<owned::Segments<'d>>,
        buffered::Rejected<segments::Chain<resident::Shared<'d>>, owned::Segments<'d>, O>,
    > {
        self.inner.try_finish(ledger, &mut evidence)
    }
}

pub(super) fn retain<'d>(
    mut current: Option<segments::Chain<resident::Shared<'d>>>,
    data: event::DataPayload<resident::Shared<'d>>,
) -> buffered::Retain<
    segments::Chain<resident::Shared<'d>>,
    event::DataPayload<resident::Shared<'d>>,
    segments::Error,
> {
    match data.try_append(&mut current) {
        Ok(()) => buffered::Retain::Accepted { storage: current },
        Err(rejected) => {
            let (input, error) = rejected.into_parts();
            buffered::Retain::Rejected {
                storage: current,
                input,
                error,
            }
        }
    }
}

pub(super) fn classify<D>(pushed: buffered::Push<D, segments::Error>) -> Push<D> {
    match pushed {
        buffered::Push::Accepted | buffered::Push::Discarded(_) => Push::Accepted,
        buffered::Push::Rejected { input, error } => {
            let error = match error {
                buffered::Error::Body(error) => payload::Error::Body(error),
                buffered::Error::Retain(error) => payload::Error::Segments(error),
            };
            Push::Rejected { input, error }
        }
    }
}
