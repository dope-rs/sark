use std::time;

use dope::net::link::egress::load;
use o3::{
    alloc,
    buffer::{storage, write::consume},
};
use sark_http::h1::head;

use crate::{frame, inbound};

#[derive(Debug)]
pub enum Error {
    Open(head::ParseError<crate::upgrade::Error>),
    Handshake(crate::upgrade::Error),
    Forbidden,
    OpenTail {
        actual: usize,
    },
    OpenTimeout,
    OpenTimerDisabled,
    OpenTimerSealed,
    OpenTimerRange {
        now: time::Instant,
        window: time::Duration,
    },
    Frame(inbound::Fault),
    Plan(frame::PlanError),
    Capacity(consume::Error),
    Allocate(storage::AllocError),
    Reserve(alloc::Error),
    Egress(load::Error),
}

impl From<consume::Error> for Error {
    fn from(error: consume::Error) -> Self {
        Self::Capacity(error)
    }
}

impl From<frame::PlanError> for Error {
    fn from(error: frame::PlanError) -> Self {
        Self::Plan(error)
    }
}
