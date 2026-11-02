use core::{convert::Infallible, marker::PhantomData};

use sark_core::{
    body::{buffered, credit},
    decode::{
        exec, request,
        state::{self, Family, Set, open},
    },
};
use sark_http::payload::owned;

pub(in crate::manifold) struct Receiving<'d>(PhantomData<&'d ()>);

impl<'d> Set for Receiving<'d> {
    type SyncOpen = buffered::State<owned::Segments<'d>>;
    type SyncFinished = Infallible;
    type TaskOpen = buffered::State<owned::Segments<'d>>;
    type TaskFinished = Infallible;
    type DuplexOpen = Infallible;
    type DuplexFinished = Infallible;
    type UnsupportedOpen = Infallible;
    type UnsupportedFinished = Infallible;
}

impl<'d> open::Set for Receiving<'d> {
    type Body = buffered::State<owned::Segments<'d>>;
}

pub(in crate::manifold) struct Buffered<'d, P: request::Prepared> {
    pub(super) request: <P::Family as Family<P>>::State<Receiving<'d>>,
}

pub(in crate::manifold) enum Finished<'d, P> {
    Sync(request::Ready<exec::Sync, P, owned::Segments<'d>>),
    Task(request::Ready<exec::Task, P, owned::Segments<'d>>),
}

impl<'d, P: request::Prepared> Buffered<'d, P> {
    pub(in crate::manifold) const fn new(
        request: <P::Family as Family<P>>::State<Receiving<'d>>,
    ) -> Self {
        Self { request }
    }

    pub(in crate::manifold::requests) fn into_release(self) -> credit::Release {
        state::Flow::consume(self.request, buffered::State::into_release)
    }
}
