use core::{convert::Infallible, marker::PhantomData};

use o3::buffer::{resident, segments};
use sark_core::decode::{
    request,
    state::{self, open},
};

use crate::server::{payload::Buffer, state::exchange::Input};

pub(in crate::server::task) struct Receiving<'d>(PhantomData<&'d ()>);

impl<'d> state::Set for Receiving<'d> {
    type SyncOpen = Buffer<'d>;
    type SyncFinished = Infallible;
    type TaskOpen = Buffer<'d>;
    type TaskFinished = Infallible;
    type DuplexOpen = Infallible;
    type DuplexFinished = Infallible;
    type UnsupportedOpen = Infallible;
    type UnsupportedFinished = Infallible;
}

impl<'d> open::Set for Receiving<'d> {
    type Body = Buffer<'d>;
}

impl<'d> Input<'d> for Receiving<'d> {
    fn sync(body: Buffer<'d>) -> Buffer<'d> {
        body
    }

    fn task(body: Buffer<'d>) -> Buffer<'d> {
        body
    }

    fn sync_done(never: Infallible) -> Buffer<'d> {
        match never {}
    }

    fn task_done(never: Infallible) -> Buffer<'d> {
        match never {}
    }

    fn duplex(never: Infallible) -> Option<segments::Chain<resident::Shared<'d>>> {
        match never {}
    }

    fn duplex_done(never: Infallible) -> Option<segments::Chain<resident::Shared<'d>>> {
        match never {}
    }

    fn other(never: Infallible) -> Buffer<'d> {
        match never {}
    }

    fn other_done(never: Infallible) -> Buffer<'d> {
        match never {}
    }
}

pub(in crate::server::task) struct Buffered<'d, P>
where
    P: request::Prepared,
{
    pub(super) request: <P::Family as state::Family<P>>::State<Receiving<'d>>,
}

impl<'d, P> Buffered<'d, P>
where
    P: request::Prepared,
{
    pub(in crate::server::task) const fn new(
        request: <P::Family as state::Family<P>>::State<Receiving<'d>>,
    ) -> Self {
        Self { request }
    }
}
