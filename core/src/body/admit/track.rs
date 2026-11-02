use o3::mem::Rejected;

use crate::{body, body::admit, manifold::stream::Boundary};

/// Linear length proof coupled to its admission phase.
///
#[derive(Debug)]
pub struct Track<P> {
    bound: admit::Bound<P>,
    pub(super) len: usize,
}

impl<P> Track<P> {
    pub const fn bound(&self) -> usize {
        self.bound.value
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub const fn retention(&self) -> admit::Retention {
        self.bound.retention
    }

    pub const fn stream(&self) -> admit::Stream {
        self.bound.stream
    }

    fn try_check(&self) -> Result<(), body::Error> {
        if matches!(self.bound.kind, admit::Kind::Exact) && self.len != self.bound() {
            return Err(body::Error::LengthMismatch {
                actual: self.len,
                declared: self.bound(),
            });
        }
        Ok(())
    }

    fn try_next(&self, added: usize) -> Result<usize, body::Error> {
        let next = self.len.checked_add(added).ok_or(body::Error::Overflow {
            current: self.len,
            added,
        })?;
        if next > self.bound() {
            return Err(match self.bound.kind {
                admit::Kind::Exact => body::Error::LengthMismatch {
                    actual: next,
                    declared: self.bound(),
                },
                admit::Kind::Limit => body::Error::TooLarge {
                    actual: next,
                    limit: self.bound(),
                },
            });
        }
        Ok(next)
    }
}

impl Track<admit::Open> {
    pub(super) const fn new(bound: admit::Bound<admit::Open>) -> Self {
        Self { bound, len: 0 }
    }

    pub(in crate::body) fn into_flow(self) -> Track<Boundary> {
        Track {
            bound: self.bound.map(|admit::Open| Boundary::More),
            len: self.len,
        }
    }

    pub(super) fn try_admit(mut self, len: usize) -> Result<Track<admit::Finished>, body::Error> {
        self.len = self.try_next(len)?;
        self.try_complete()
    }

    pub(super) fn try_complete(self) -> Result<Track<admit::Finished>, body::Error> {
        self.try_check()?;
        Ok(Track {
            bound: self.bound.map(|admit::Open| admit::Finished),
            len: self.len,
        })
    }
}

impl Track<admit::Finished> {
    pub(in crate::body) fn into_flow(self) -> Track<Boundary> {
        Track {
            bound: self.bound.map(|admit::Finished| Boundary::End),
            len: self.len,
        }
    }
}

impl Track<Boundary> {
    pub(in crate::body) const fn boundary(&self) -> Boundary {
        self.bound.phase
    }

    pub(in crate::body) fn try_advance(
        &mut self,
        added: usize,
    ) -> Result<admit::Advance<'_>, body::Error> {
        match self.bound.phase {
            Boundary::End => return Err(body::Error::InputFinished),
            Boundary::More => {}
        }
        let next = self.try_next(added)?;
        Ok(admit::Advance { track: self, next })
    }

    pub(in crate::body) fn try_end(&mut self) -> Result<usize, body::Error> {
        match self.bound.phase {
            Boundary::End => return Err(body::Error::InputFinished),
            Boundary::More => {}
        }
        self.try_check()?;
        self.bound.phase = Boundary::End;
        Ok(self.len)
    }

    pub(in crate::body) fn try_complete(
        self,
    ) -> Result<Track<admit::Finished>, Rejected<Self, body::Error>> {
        if matches!(self.bound.phase, Boundary::More)
            && let Err(error) = self.try_check()
        {
            return Err(Rejected::new(self, error));
        }
        Ok(Track {
            bound: self.bound.map(|_| admit::Finished),
            len: self.len,
        })
    }
}

const _: () = {
    use core::mem::size_of;
    assert!(size_of::<Track<admit::Open>>() == 3 * size_of::<usize>());
    assert!(size_of::<Track<admit::Finished>>() == 3 * size_of::<usize>());
    assert!(size_of::<Track<Boundary>>() == 3 * size_of::<usize>());
    assert!(size_of::<Option<Track<admit::Open>>>() == size_of::<Track<admit::Open>>());
    assert!(size_of::<Option<Track<admit::Finished>>>() == size_of::<Track<admit::Finished>>());
    assert!(size_of::<Option<Track<Boundary>>>() == size_of::<Track<Boundary>>());
};
