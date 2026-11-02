use std::marker::PhantomData;

use handoff::{Ready, State, stage};
use o3::buffer::storage::{self, backing};

use crate::{
    owner::handoff,
    payload::{fixed, owned},
    request::{self, Id, body},
};

#[derive(Clone, Copy)]
pub(crate) struct Origin<'req, 'd, B = body::Retained<'req, 'd>> {
    pub(in crate::owner::handoff) source: Source<'req, 'd, B>,
}

#[derive(Clone, Copy)]
pub(in crate::owner::handoff) enum Source<'req, 'd, B = body::Retained<'req, 'd>> {
    Empty,
    /// A borrowed projection carries no owner that a response can link to.
    /// Its bytes are valid for `'req` within the projection domain `'d`;
    /// this arm preserves that relation.
    Borrowed(PhantomData<&'req &'d ()>),
    Retained(B),
    Task(Id<'d>),
}

impl<'req, 'd> Origin<'req, 'd> {
    pub(crate) const fn new(body: body::Ref<'req, 'd>) -> Self {
        let source = match body.try_retained() {
            Ok(body) => Source::Retained(body),
            Err(_) => Source::Borrowed(PhantomData),
        };
        Self { source }
    }
}

impl<'req, 'd, B: stage::Body<'req, 'd>> Origin<'req, 'd, B> {
    pub(crate) const fn empty() -> Self {
        Self {
            source: Source::Empty,
        }
    }

    pub(crate) const fn task(id: Id<'d>) -> Self {
        Self {
            source: Source::Task(id),
        }
    }

    pub(crate) const fn clear<S>(self) -> State<'req, 'd, S, B> {
        State::Ready {
            origin: self,
            body: Ready::Empty,
        }
    }

    pub(crate) fn owned<S>(self, body: Vec<u8>) -> State<'req, 'd, S, B> {
        if body.is_empty() {
            self.clear()
        } else {
            State::Ready {
                origin: self,
                body: Ready::Owned(body),
            }
        }
    }

    pub(crate) fn shared<S>(self, body: storage::Shared) -> State<'req, 'd, S, B> {
        if body.is_empty() {
            self.clear()
        } else {
            State::Ready {
                origin: self,
                body: Ready::Retained(body),
            }
        }
    }

    pub(crate) fn segmented<S>(self, body: owned::Segments<'d>) -> State<'req, 'd, S, B> {
        if body.is_empty() {
            self.clear()
        } else {
            State::Ready {
                origin: self,
                body: Ready::Segmented(body),
            }
        }
    }

    pub(crate) fn request<S>(self, body: request::Segments<'d>) -> State<'req, 'd, S, B> {
        if body.is_empty() {
            return self.clear();
        }
        let (id, body) = body.into_parts();
        let linked = matches!(self.source, Source::Task(expected) if expected == id);
        State::Ready {
            origin: self,
            body: if linked {
                Ready::Linked(body)
            } else {
                Ready::Segmented(body)
            },
        }
    }

    pub(crate) fn fixed<S>(self, body: fixed::Body<'d>) -> State<'req, 'd, S, B> {
        if body.is_empty() {
            self.clear()
        } else {
            State::Ready {
                origin: self,
                body: Ready::Fixed(body),
            }
        }
    }
}

impl<'req, 'd, B: stage::Body<'req, 'd>> Source<'req, 'd, B> {
    pub(in crate::owner::handoff) fn is_linked(self, response: backing::View<'_, '_>) -> bool {
        if let Self::Retained(request) = self {
            return request.borrow().contains(response);
        }
        false
    }
}

const _: () = assert!(std::mem::size_of::<storage::Shared>() <= 4 * std::mem::size_of::<usize>());
