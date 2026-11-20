use std::{convert::Infallible, ops::ControlFlow, ptr};

use handoff::{Error, Origin, Pending, Ready, stage};
use o3::buffer::{
    bytes::scope,
    hold::{self, evidence},
    storage,
};

use crate::{owner::handoff, payload::owned, request::body};

/// A selected response body, independent of the temporary header borrow.
#[repr(transparent)]
#[must_use = "resolve or finish the response owner handoff"]
#[doc(hidden)]
pub struct Plan<'req, 'd, S = Infallible, B = body::Retained<'req, 'd>> {
    state: State<'req, 'd, S, B>,
}

impl<'req, 'd, S, B: stage::Body<'req, 'd>> Plan<'req, 'd, S, B> {
    const fn new(state: State<'req, 'd, S, B>) -> Self {
        Self { state }
    }

    pub(super) const fn ready(origin: Origin<'req, 'd, B>, body: Ready<'d>) -> Self {
        Self::new(State::Ready { origin, body })
    }

    pub(crate) fn try_borrow<H: scope::Source<Selection = S>>(
        slot: &handoff::Slot<'req, 'd, H, B>,
        bytes: &[u8],
    ) -> Result<Self, owned::Error> {
        match Self::try_select(slot, bytes)? {
            Some(plan) => Ok(plan),
            None => Self::try_copy(slot.origin, bytes),
        }
    }

    pub(crate) fn try_borrow_into<
        H: scope::Source<Selection = S>,
        L: std::borrow::BorrowMut<usize>,
    >(
        slot: &handoff::Slot<'req, 'd, H, B>,
        bytes: &[u8],
        out: &mut o3::buffer::write::Slice<'_, L>,
    ) -> Result<ControlFlow<usize, Self>, owned::Error> {
        if let Some(plan) = Self::try_select(slot, bytes)? {
            return Ok(ControlFlow::Continue(plan));
        }
        if let Some(written) = handoff::copy::into(out, bytes) {
            return Ok(ControlFlow::Break(written));
        }
        Self::try_copy(slot.origin, bytes).map(ControlFlow::Continue)
    }

    fn try_select<H: scope::Source<Selection = S>>(
        slot: &handoff::Slot<'req, 'd, H, B>,
        bytes: &[u8],
    ) -> Result<Option<Self>, owned::Error> {
        let origin = slot.origin;
        if bytes.is_empty() {
            return Ok(Some(slot.clear()));
        }
        if let Origin::Retained(body) = origin {
            if body
                .borrow()
                .as_slice()
                .is_some_and(|whole| ptr::eq(whole, bytes))
            {
                return Ok(Some(Self::new(State::Body(body))));
            }
            if let Some(body) = body.borrow().try_slice(bytes)? {
                return Ok(Some(Self::ready(origin, Ready::Linked(body))));
            }
        }
        if let Some(selected) = slot.head.select(bytes) {
            return Ok(Some(Self::new(State::Head { origin, selected })));
        }
        Ok(None)
    }

    pub(crate) fn try_view<H: scope::Source<Selection = S>>(
        slot: &handoff::Slot<'req, 'd, H, B>,
        view: body::Ref<'_, 'd>,
    ) -> Result<Self, owned::Error> {
        if view.is_empty() {
            return Ok(slot.clear());
        }
        let body = match view.try_retained() {
            Ok(view) => {
                if let Origin::Retained(body) = slot.origin
                    && view.is_same(*body.borrow())
                {
                    return Ok(Self::new(State::Body(body)));
                }
                Ready::Segmented(view.try_clone()?)
            }
            Err(view) => Ready::Copied(handoff::copy::Body::new(view.try_copy()?)),
        };
        Ok(Self::ready(slot.origin, body))
    }

    fn try_copy(origin: Origin<'req, 'd, B>, bytes: &[u8]) -> Result<Self, owned::Error> {
        use handoff::copy;

        let body = storage::Shared::try_copy(bytes).map_err(owned::Error::Storage)?;
        Ok(Self::ready(origin, Ready::Copied(copy::Body::new(body))))
    }

    pub fn stage(self) -> stage::State<'d, S, B> {
        match self.state {
            State::Ready { body, .. } => stage::State::Ready(body),
            State::Body(body) => stage::State::Body(body),
            State::Head { selected, .. } => stage::State::Head(selected),
        }
    }
}

impl<'req, 'd, B: stage::Body<'req, 'd>> Plan<'req, 'd, Infallible, B> {
    pub fn detach(self) -> Pending<'d, evidence::Off, B::Deferred>
    where
        B::Deferred: stage::Deferred<'d>,
    {
        match self.state {
            State::Ready { body, .. } => Pending::ready(body, evidence::Off),
            State::Body(body) => Pending::body(body.defer(), evidence::Off),
            State::Head { selected, .. } => match selected {},
        }
    }

    pub fn try_detach(self) -> Result<Pending<'d, evidence::On, B::Deferred>, Error>
    where
        B::Deferred: stage::Deferred<'d>,
    {
        match self.state {
            State::Ready { origin, body } => {
                let charge = body.try_linked(origin).map_err(Error::Overflow)?;
                Ok(Pending::ready(body, charge))
            }
            State::Body(body) => Ok(Pending::body(body.defer(), body.borrow().charge())),
            State::Head { selected, .. } => match selected {},
        }
    }

    pub fn finish(self) -> Result<Ready<'d>, Error> {
        match self.state {
            State::Ready { body, .. } => Ok(body),
            State::Body(body) => body
                .borrow()
                .try_clone()
                .map(Ready::Linked)
                .map_err(Error::Retain),
            State::Head { selected, .. } => match selected {},
        }
    }

    pub fn try_finish(self) -> Result<(Ready<'d>, hold::Charge), Error> {
        match self.state {
            State::Ready { origin, body } => {
                let charge = body.try_linked(origin).map_err(Error::Overflow)?;
                Ok((body, charge))
            }
            State::Body(body) => {
                let body = body.borrow();
                let charge = body.charge();
                let body = body.try_clone().map_err(Error::Retain)?;
                Ok((Ready::Linked(body), charge))
            }
            State::Head { selected, .. } => match selected {},
        }
    }
}

const _: () = assert!(size_of::<Plan<'static, 'static>>() == size_of::<State<'static, 'static>>());

enum State<'req, 'd, S = Infallible, B = body::Retained<'req, 'd>> {
    Ready {
        origin: Origin<'req, 'd, B>,
        body: Ready<'d>,
    },
    Body(B),
    Head {
        origin: Origin<'req, 'd, B>,
        selected: S,
    },
}

const _: () = assert!(size_of::<State<'static, 'static>>() <= 11 * size_of::<usize>());

impl<'req, 'd, 'id, B: stage::Body<'req, 'd>> Plan<'req, 'd, scope::Range<'id>, B> {
    pub fn resolve_head<O: Into<scope::Owner<'id, storage::Shared>>>(
        self,
        head: O,
    ) -> Plan<'req, 'd, Infallible, B> {
        Plan::new(match self.state {
            State::Ready { origin, body } => State::Ready { origin, body },
            State::Body(body) => State::Body(body),
            State::Head { origin, selected } => State::Ready {
                origin,
                body: Ready::Retained(head.into().project(selected)),
            },
        })
    }
}
