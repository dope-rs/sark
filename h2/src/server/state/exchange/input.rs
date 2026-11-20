use o3::buffer::{resident, segments};
use sark_core::decode::state;

use crate::server::{payload::Buffer, state::exchange::Pending};

pub(crate) trait Input<'d>:
    state::Set<
        SyncOpen: Into<Buffer<'d>>,
        SyncFinished: Into<Buffer<'d>>,
        TaskOpen = <Self as state::Set>::SyncOpen,
        TaskFinished = <Self as state::Set>::SyncFinished,
        UnsupportedOpen: Into<Buffer<'d>>,
        UnsupportedFinished = <Self as state::Set>::SyncFinished,
        DuplexFinished = <Self as state::Set>::DuplexOpen,
    >
{
    fn duplex(body: Self::DuplexOpen) -> Option<segments::Chain<resident::Shared<'d>>>;
}

impl<'d> Input<'d> for state::Routed {
    fn duplex((): ()) -> Option<segments::Chain<resident::Shared<'d>>> {
        None
    }
}

impl<'d> Input<'d> for Pending<'d> {
    fn duplex(body: Self::DuplexOpen) -> Option<segments::Chain<resident::Shared<'d>>> {
        body
    }
}
