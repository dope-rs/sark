use core::mem;

use o3::buffer::segments::assembly;
use sark_core::{
    body::{self, admit, credit},
    route,
};
use sark_http::payload::{owned, response::Body as _};

use crate::wire::body::View;

/// Retained request-body ownership for a suspended routed continuation.
#[repr(transparent)]
pub(crate) struct Body<'d> {
    buf: owned::Segments<'d>,
}

impl<'d> Body<'d> {
    pub(crate) const fn new() -> Self {
        Self {
            buf: owned::Segments::new(),
        }
    }

    pub(crate) fn push<E: credit::Evidence>(
        &mut self,
        parts: owned::Segments<'d>,
        mut proof: E,
    ) -> Result<(), assembly::Error> {
        let credit = E::credit(parts.len(), parts.count());
        let (body, result) =
            match mem::replace(&mut self.buf, owned::Segments::new()).try_extend(parts) {
                Ok(body) => {
                    proof.reserved(credit);
                    (body, Ok(()))
                }
                Err(rejected) => {
                    let ((body, _), error) = rejected.into_parts();
                    (body, Err(error))
                }
            };
        self.buf = body;
        result
    }

    pub(crate) fn matches(&self, mode: admit::Retention, total: usize) -> bool {
        self.buf.len()
            == match mode {
                admit::Retention::Discarded => 0,
                admit::Retention::Retained => total,
            }
    }

    pub(crate) fn try_admit<R: route::Spec, E: credit::Evidence>(
        &mut self,
        total: usize,
        want: admit::Length,
        proof: &mut E,
    ) -> Result<body::Body<View<'_, 'd>>, admit::Error<View<'_, 'd>>> {
        let credit = E::credit(self.buf.len(), self.buf.count());
        let result = View::from_body(&mut self.buf).finish::<R>(total, want);
        proof.released(
            credit,
            match &result {
                Ok(_) => credit::Disposition::Admitted,
                Err(_) => credit::Disposition::Discarded,
            },
        );
        result
    }

    pub(crate) fn discard<E: credit::Evidence>(self, mut proof: E) {
        let credit = {
            let body = self;
            E::credit(body.buf.len(), body.buf.count())
        };
        proof.released(credit, credit::Disposition::Discarded);
    }

    pub(crate) fn finish(self) -> owned::Segments<'d> {
        self.buf
    }
}

const _: () = {
    assert!(
        core::mem::size_of::<Body<'static>>() == core::mem::size_of::<owned::Segments<'static>>()
    );
    assert!(
        core::mem::align_of::<Body<'static>>() == core::mem::align_of::<owned::Segments<'static>>()
    );
};
