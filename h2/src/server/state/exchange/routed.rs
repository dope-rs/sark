use core::{convert::Infallible, marker::PhantomData, ops::ControlFlow};

use o3::buffer::{resident, segments};
use sark_core::{
    body::{self, admit},
    decode::{request, state},
};

use crate::server::{payload, payload::Buffer, state::exchange::Input};

pub(crate) struct Pending<'d>(PhantomData<&'d ()>);

impl<'d> state::Set for Pending<'d> {
    type SyncOpen = Buffer<'d>;
    type SyncFinished = Buffer<'d>;
    type TaskOpen = Buffer<'d>;
    type TaskFinished = Buffer<'d>;
    type DuplexOpen = Option<segments::Chain<resident::Shared<'d>>>;
    type DuplexFinished = Option<segments::Chain<resident::Shared<'d>>>;
    type UnsupportedOpen = Buffer<'d>;
    type UnsupportedFinished = Buffer<'d>;
}

pub(crate) struct Accepted<'d>(PhantomData<&'d ()>);

impl<'d> state::Set for Accepted<'d> {
    type SyncOpen = Buffer<'d>;
    type SyncFinished = Buffer<'d>;
    type TaskOpen = Buffer<'d>;
    type TaskFinished = Buffer<'d>;
    type DuplexOpen = Option<segments::Chain<resident::Shared<'d>>>;
    type DuplexFinished = Option<segments::Chain<resident::Shared<'d>>>;
    type UnsupportedOpen = Buffer<'d>;
    type UnsupportedFinished = Buffer<'d>;
}

impl<'d> state::Finish for Pending<'d> {
    fn sync(body: Buffer<'d>) -> Buffer<'d> {
        body
    }

    fn task(body: Buffer<'d>) -> Buffer<'d> {
        body
    }

    fn duplex(
        body: Option<segments::Chain<resident::Shared<'d>>>,
    ) -> Option<segments::Chain<resident::Shared<'d>>> {
        body
    }

    fn reject(body: Buffer<'d>) -> Buffer<'d> {
        body
    }
}

struct Prepare<'d>(PhantomData<&'d ()>);

impl<'d, P> state::Map<P, state::Routed, Pending<'d>> for Prepare<'d> {
    type Break = Infallible;
    type Retained = ();

    fn sync_open<S: state::Slot<P>>(
        self,
        slot: S,
        body: admit::Track<admit::Open>,
    ) -> ControlFlow<Infallible, ((), S, Buffer<'d>)> {
        ControlFlow::Continue(((), slot, <state::Routed as Input<'d>>::sync(body)))
    }

    fn sync_done<S: state::Slot<P>>(
        self,
        slot: S,
        body: admit::Track<admit::Finished>,
    ) -> ControlFlow<Infallible, ((), S, Buffer<'d>)> {
        ControlFlow::Continue(((), slot, <state::Routed as Input<'d>>::sync_done(body)))
    }

    fn task_open<S: state::Slot<P>>(
        self,
        slot: S,
        body: admit::Track<admit::Open>,
    ) -> ControlFlow<Infallible, ((), S, Buffer<'d>)> {
        ControlFlow::Continue(((), slot, <state::Routed as Input<'d>>::task(body)))
    }

    fn task_done<S: state::Slot<P>>(
        self,
        slot: S,
        body: admit::Track<admit::Finished>,
    ) -> ControlFlow<Infallible, ((), S, Buffer<'d>)> {
        ControlFlow::Continue(((), slot, <state::Routed as Input<'d>>::task_done(body)))
    }

    fn duplex_open<S: state::Slot<P>>(
        self,
        slot: S,
        (): (),
    ) -> ControlFlow<Infallible, ((), S, Option<segments::Chain<resident::Shared<'d>>>)> {
        ControlFlow::Continue(((), slot, <state::Routed as Input<'d>>::duplex(())))
    }

    fn duplex_done<S: state::Slot<P>>(
        self,
        slot: S,
        (): (),
    ) -> ControlFlow<Infallible, ((), S, Option<segments::Chain<resident::Shared<'d>>>)> {
        ControlFlow::Continue(((), slot, <state::Routed as Input<'d>>::duplex_done(())))
    }

    fn reject_open<S: state::Slot<P>>(
        self,
        slot: S,
        body: admit::Track<admit::Open>,
    ) -> ControlFlow<Infallible, ((), S, Buffer<'d>)> {
        ControlFlow::Continue(((), slot, <state::Routed as Input<'d>>::other(body)))
    }

    fn reject_done<S: state::Slot<P>>(
        self,
        slot: S,
        body: admit::Track<admit::Finished>,
    ) -> ControlFlow<Infallible, ((), S, Buffer<'d>)> {
        ControlFlow::Continue(((), slot, <state::Routed as Input<'d>>::other_done(body)))
    }
}

pub(crate) struct Routed<'d, P, F>
where
    P: request::Prepared,
    F: state::Family<P>,
{
    pub(super) request: request::Guarded<F::State<Pending<'d>>>,
}

impl<'d, P, F> Routed<'d, P, F>
where
    P: request::Prepared,
    F: state::Family<P>,
{
    pub(crate) fn new<T>(request: request::Guarded<T>) -> Self
    where
        T: state::Flow<P, state::Routed, Family = F>,
    {
        let request = request.map(|request| {
            let ControlFlow::Continue(request) = state::Flow::map(request, Prepare(PhantomData));
            request
        });
        Self { request }
    }

    pub(crate) fn into_state(self) -> request::Guarded<F::State<Pending<'d>>> {
        self.request
    }
}

pub(crate) trait Deferred<'d, P: request::Prepared, F: state::Family<P>>: Sized {
    fn try_finish(self) -> Result<Self, (payload::Error, Self)>;

    fn route_mut(&mut self) -> &mut Routed<'d, P, F>;

    fn into_route(self) -> Routed<'d, P, F>;
}

impl<'d, P: request::Prepared, F: state::Family<P>> Deferred<'d, P, F> for Infallible {
    fn try_finish(self) -> Result<Self, (payload::Error, Self)> {
        match self {}
    }

    fn route_mut(&mut self) -> &mut Routed<'d, P, F> {
        match *self {}
    }

    fn into_route(self) -> Routed<'d, P, F> {
        match self {}
    }
}

impl<'d, P: request::Prepared, F: state::Family<P>> Deferred<'d, P, F> for Routed<'d, P, F> {
    fn try_finish(self) -> Result<Self, (payload::Error, Self)> {
        let (request, method) = self.request.into_parts();
        match state::Flow::finish(request) {
            Ok(request) => Ok(Self {
                request: request::Guarded::new(request, method),
            }),
            Err(request) => Err((
                payload::Error::Body(body::Error::InputFinished),
                Self {
                    request: request::Guarded::new(request, method),
                },
            )),
        }
    }

    fn route_mut(&mut self) -> &mut Routed<'d, P, F> {
        self
    }

    fn into_route(self) -> Routed<'d, P, F> {
        self
    }
}
