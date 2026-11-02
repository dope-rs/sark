use o3::{base::cell::region, buffer::segments, mem::Rejected};

use crate::{
    body::{self, admit, credit},
    manifold::stream::Boundary,
};

#[derive(Debug)]
pub struct State<O: credit::Origin = credit::raw::Stored> {
    pub(in crate::body) track: admit::Track<Boundary>,
    pub(super) credit: credit::Amount,
    pub(super) origin: O,
}

/// The exact ingress owner after its transport boundary has finished.
pub(super) struct Finished<O: credit::Origin> {
    pub(in crate::body) state: State<O>,
}

impl<O: credit::Origin> Finished<O> {
    pub(super) fn into_state(self) -> State<O> {
        self.state
    }

    pub(super) fn try_drain<E: credit::Evidence>(
        &mut self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: E,
    ) -> Result<(), body::Error> {
        Turn::new(&mut self.state, ledger, event).try_drain(credit::Disposition::Admitted)
    }
}

impl<O: credit::Origin> State<O> {
    pub(super) fn new(start: admit::Start, origin: O) -> Self {
        let track = match start {
            admit::Start::Open(open) => open.into_flow(),
            admit::Start::Finished(done) => done.into_flow(),
        };
        Self {
            track,
            credit: credit::Amount::new(0, segments::Count::EMPTY),
            origin,
        }
    }

    pub const fn max_body(&self) -> usize {
        self.track.bound()
    }

    pub(super) const fn retention(&self) -> admit::Retention {
        self.track.retention()
    }

    pub(super) const fn stream(&self) -> admit::Stream {
        self.track.stream()
    }

    pub(super) const fn logical_len(&self) -> usize {
        self.track.len()
    }

    pub(super) const fn is_finished(&self) -> bool {
        matches!(self.track.boundary(), Boundary::End)
    }

    pub const fn credit(&self) -> credit::Amount {
        self.credit
    }

    pub(super) fn try_ready(self) -> Result<Finished<O>, Self> {
        match self.track.boundary() {
            Boundary::End => Ok(Finished { state: self }),
            Boundary::More => Err(self),
        }
    }

    pub(super) fn try_end(&mut self) -> Result<usize, body::Error> {
        self.track.try_end()
    }

    pub(super) fn try_exact(&mut self, len: usize) -> Result<(), body::Error> {
        if self.retention() == admit::Retention::Discarded {
            let left = len
                .checked_sub(self.logical_len())
                .ok_or(body::Error::SegmentMismatch)?;
            self.track.try_advance(left)?.commit();
        } else if len != self.logical_len() {
            return Err(body::Error::SegmentMismatch);
        }
        Ok(())
    }

    pub(super) fn try_finish(
        self,
    ) -> Result<(admit::Track<admit::Finished>, credit::Release<O>), Rejected<Self, body::Error>>
    {
        let Self {
            track,
            credit,
            origin,
        } = self;
        match track.try_complete() {
            Ok(track) => Ok((track, credit::Release::new(credit, origin))),
            Err(failure) => {
                let (track, error) = failure.into_parts();
                Err(Rejected::new(
                    Self {
                        track,
                        credit,
                        origin,
                    },
                    error,
                ))
            }
        }
    }

    pub(super) const fn abandon(self) -> credit::Release<O> {
        credit::Release::new(self.credit, self.origin)
    }
}

pub(in crate::body) struct Advance<'a, E> {
    track: admit::Advance<'a>,
    credit: &'a mut credit::Amount,
    ledger: &'a mut credit::Amount,
    event: &'a mut E,
}

pub(in crate::body) struct Reserve<'a, E> {
    track: admit::Advance<'a>,
    credit: credit::Change<'a>,
    event: &'a mut E,
}

impl<'a, E: credit::Evidence> Reserve<'a, E> {
    pub(in crate::body) fn commit(self) -> admit::Advance<'a> {
        self.event.reserved(self.credit.commit());
        self.track
    }
}

impl<'a, E: credit::Evidence> Advance<'a, E> {
    pub(in crate::body) fn try_reserve(
        self,
        amount: credit::Amount,
        rmax: credit::Limits,
        cmax: credit::Limits,
    ) -> Result<Reserve<'a, E>, credit::Error> {
        Ok(Reserve {
            track: self.track,
            credit: self.ledger.try_commit(self.credit, amount, rmax, cmax)?,
            event: self.event,
        })
    }

    pub(in crate::body) fn commit(self) {
        self.track.commit();
    }
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<State>() == 5 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<State<region::Proof<region::Identity<'static>>>>()
            == mem::size_of::<State>()
    );
    assert!(mem::size_of::<Reserve<'_, credit::Disabled>>() == 11 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Option<Reserve<'_, credit::Disabled>>>()
            == mem::size_of::<Reserve<'_, credit::Disabled>>()
    );
};

pub struct Turn<'state, 'ledger, E, O: credit::Origin> {
    state: &'state mut State<O>,
    ledger: &'ledger mut credit::Amount,
    pub(super) event: E,
}

impl<'state, 'ledger, E: credit::Evidence, O: credit::Origin> Turn<'state, 'ledger, E, O> {
    pub fn new(
        state: &'state mut State<O>,
        ledger: &'ledger mut credit::raw::Ledger<'_, O>,
        event: E,
    ) -> Self {
        Self {
            state,
            ledger: ledger.amount(),
            event,
        }
    }

    pub(in crate::body) fn from_account(
        state: &'state mut State<O>,
        ledger: &'ledger mut credit::Amount,
        event: E,
    ) -> Self {
        Self {
            state,
            ledger,
            event,
        }
    }

    pub fn state(&self) -> &State<O> {
        self.state
    }

    pub(super) fn state_mut(&mut self) -> &mut State<O> {
        self.state
    }

    pub(in crate::body) fn try_advance(
        &mut self,
        added: usize,
    ) -> Result<Advance<'_, E>, body::Error> {
        let track = self.state.track.try_advance(added)?;
        Ok(Advance {
            track,
            credit: &mut self.state.credit,
            ledger: &mut *self.ledger,
            event: &mut self.event,
        })
    }

    pub(in crate::body) fn try_release(
        &mut self,
        amount: credit::Amount,
        kind: credit::Disposition,
    ) -> Result<(), credit::Error> {
        let change = self.ledger.try_release(&mut self.state.credit, amount)?;
        let freed = change.commit();
        self.event.released(freed, kind);
        Ok(())
    }

    pub fn try_drain(&mut self, kind: credit::Disposition) -> Result<(), body::Error> {
        let change = self
            .ledger
            .try_drain(&mut self.state.credit)
            .map_err(body::Error::Credit)?;
        let freed = change.commit();
        self.event.released(freed, kind);
        Ok(())
    }
}
