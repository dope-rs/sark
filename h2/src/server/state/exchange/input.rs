use o3::buffer::{resident, segments};
use sark_core::{body::admit, decode::state};

use crate::server::{payload::Buffer, state::exchange::Pending};

pub(crate) trait Input<'d>: state::Set {
    fn sync(body: Self::SyncOpen) -> Buffer<'d>;

    fn sync_done(body: Self::SyncFinished) -> Buffer<'d>;

    fn task(body: Self::TaskOpen) -> Buffer<'d>;

    fn task_done(body: Self::TaskFinished) -> Buffer<'d>;

    fn duplex(body: Self::DuplexOpen) -> Option<segments::Chain<resident::Shared<'d>>>;

    fn duplex_done(body: Self::DuplexFinished) -> Option<segments::Chain<resident::Shared<'d>>>;

    /// Releases a duplex body on a close path that never delivers it.
    ///
    /// The hooks return the chain a delivering path would consume, so the
    /// release lives once per hook instead of at every close arm.
    fn close_duplex(body: Self::DuplexOpen) {
        Self::duplex(body);
    }

    /// Releases a finished duplex body on a close path that never delivers it.
    fn end_duplex(body: Self::DuplexFinished) {
        Self::duplex_done(body);
    }

    fn other(body: Self::UnsupportedOpen) -> Buffer<'d>;

    fn other_done(body: Self::UnsupportedFinished) -> Buffer<'d>;
}

impl<'d> Input<'d> for state::Routed {
    fn sync(body: admit::Track<admit::Open>) -> Buffer<'d> {
        Buffer::start(admit::Start::Open(body))
    }

    fn sync_done(body: admit::Track<admit::Finished>) -> Buffer<'d> {
        Buffer::start(admit::Start::Finished(body))
    }

    fn task(body: admit::Track<admit::Open>) -> Buffer<'d> {
        Buffer::start(admit::Start::Open(body))
    }

    fn task_done(body: admit::Track<admit::Finished>) -> Buffer<'d> {
        Buffer::start(admit::Start::Finished(body))
    }

    fn duplex((): ()) -> Option<segments::Chain<resident::Shared<'d>>> {
        None
    }

    fn duplex_done((): ()) -> Option<segments::Chain<resident::Shared<'d>>> {
        None
    }

    fn other(body: admit::Track<admit::Open>) -> Buffer<'d> {
        Buffer::start(admit::Start::Open(body))
    }

    fn other_done(body: admit::Track<admit::Finished>) -> Buffer<'d> {
        Buffer::start(admit::Start::Finished(body))
    }
}

impl<'d> Input<'d> for Pending<'d> {
    fn sync(body: Buffer<'d>) -> Buffer<'d> {
        body
    }

    fn sync_done(body: Buffer<'d>) -> Buffer<'d> {
        body
    }

    fn task(body: Buffer<'d>) -> Buffer<'d> {
        body
    }

    fn task_done(body: Buffer<'d>) -> Buffer<'d> {
        body
    }

    fn duplex(
        body: Option<segments::Chain<resident::Shared<'d>>>,
    ) -> Option<segments::Chain<resident::Shared<'d>>> {
        body
    }

    fn duplex_done(
        body: Option<segments::Chain<resident::Shared<'d>>>,
    ) -> Option<segments::Chain<resident::Shared<'d>>> {
        body
    }

    fn other(body: Buffer<'d>) -> Buffer<'d> {
        body
    }

    fn other_done(body: Buffer<'d>) -> Buffer<'d> {
        body
    }
}
