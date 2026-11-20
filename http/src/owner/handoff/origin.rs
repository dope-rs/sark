use std::marker::PhantomData;

use o3::buffer::storage::{self, backing};

use crate::{
    owner::handoff::stage,
    request::{Id, body},
};

#[derive(Clone, Copy)]
pub(super) enum Origin<'req, 'd, B = body::Retained<'req, 'd>> {
    /// A borrowed projection carries no owner that a response can link to.
    /// Its bytes are valid for `'req` within the projection domain `'d`;
    /// this arm preserves that relation.
    Empty(PhantomData<&'req &'d ()>),
    Retained(B),
    Task(Id<'d>),
}

impl<'req, 'd, B: stage::Body<'req, 'd>> Origin<'req, 'd, B> {
    pub(super) fn is_linked(self, response: backing::View<'_, '_>) -> bool {
        if let Self::Retained(request) = self {
            return request.borrow().contains(response);
        }
        false
    }
}

const _: () = assert!(std::mem::size_of::<storage::Shared>() <= 4 * std::mem::size_of::<usize>());
