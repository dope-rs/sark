use core::{convert::Infallible, num::NonZero};

use o3::{
    buffer::{self, bytes::window::Snapshot, storage, write},
    mem::Rejected,
};
use scan::{Progress, line};
use write::{CopyError, SourceExt as _};

use crate::h1::head::{
    Limit, ParseError, field,
    line::{Owned, Request},
    request::{Kind, Parts, plan::Start, scan},
    state,
};

/// Append-only input and progress toward a head with a nonempty effective authority.
pub struct State<S> {
    source: S,
    inner: state::State<Kind>,
}

impl<S> State<S> {
    pub const fn new(source: S, limit: Limit) -> Self {
        Self {
            source,
            inner: state::State::new(Kind, limit),
        }
    }
}

impl<'a> State<&'a [u8]> {
    /// Continues with fields in the exact input that admitted this request line.
    pub fn try_request(request: Request<'a>, limit: Limit) -> Result<Self, ParseError<Infallible>> {
        let start = Start::try_new(request, limit)?;
        Ok(Self {
            source: request.source(),
            inner: state::State::fields(start, limit),
        })
    }
}

impl<const MAX: usize> State<Snapshot<MAX>> {
    /// Continues with fields using the line proof retained by this exact input.
    pub fn try_line(
        source: Owned<MAX>,
        limit: Limit,
    ) -> Result<Self, Rejected<Owned<MAX>, ParseError<Infallible>>> {
        let start = match Start::try_new(source.request(), limit) {
            Ok(start) => start,
            Err(error) => return Err(Rejected::new(source, error)),
        };
        Ok(Self {
            source: source.into_source(),
            inner: state::State::fields(start, limit),
        })
    }
}

impl<S: buffer::raw::Source> State<S> {
    /// Copies this exact prefix into a newly allocated append-only snapshot.
    pub fn try_snapshot<const MAX: usize>(
        &self,
        capacity: NonZero<usize>,
    ) -> Result<State<Snapshot<MAX>>, storage::AllocError> {
        let source = Snapshot::try_copy(self.source.as_ref(), capacity)?;
        Ok(State {
            source,
            inner: self.inner.clone(),
        })
    }

    /// Retains the exact incomplete line after the request-line parser needs input.
    pub fn pending_line(source: S, limit: Limit) -> Result<Self, (S, ParseError<Infallible>)> {
        match state::State::pending_line(Kind, source.as_ref(), limit) {
            Ok(inner) => Ok(Self { source, inner }),
            Err(error) => Err((source, error)),
        }
    }

    /// Scans only newly appended line bytes and transfers the unchanged source.
    pub fn advance_line(self) -> Result<line::Progress<S>, (S, ParseError<Infallible>)> {
        match self.inner.advance_line(self.source.as_ref()) {
            Ok((_, true)) => Ok(line::Progress::Complete(self.source)),
            Ok((inner, false)) => Ok(line::Progress::More(Self {
                source: self.source,
                inner,
            })),
            Err(error) => Err((self.source, error)),
        }
    }

    /// Copies the exact retained prefix without parsing any byte again.
    pub fn try_copy<D>(&self, target: D) -> Result<State<D>, (D, CopyError<D::Error>)>
    where
        D: buffer::raw::Source + write::Sink,
    {
        let target = target.try_copy(self.source.as_ref())?;
        Ok(State {
            source: target,
            inner: self.inner.clone(),
        })
    }

    pub fn advance<E>(
        self,
        sink: impl FnMut(field::Entry<'_>) -> Result<(), E>,
    ) -> Result<Progress<S>, (S, ParseError<E>)> {
        let output = self.inner.parse(self.source.as_ref(), sink);
        match output {
            Ok((_, Some(head))) => Ok(Progress::Complete(Parts::new(self.source, head))),
            Ok((inner, None)) => Ok(Progress::More(Self {
                source: self.source,
                inner,
            })),
            Err(cause) => Err((self.source, cause)),
        }
    }
}

impl<S: buffer::raw::Source> AsRef<[u8]> for State<S> {
    fn as_ref(&self) -> &[u8] {
        self.source.as_ref()
    }
}

impl<S: write::Sink> State<S> {
    pub fn try_extend(&mut self, bytes: &[u8]) -> Result<(), S::Error> {
        self.source.write_slice(bytes)
    }
}

const _: () = assert!(core::mem::size_of::<State<()>>() <= 144);
