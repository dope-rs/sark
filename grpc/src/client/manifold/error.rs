use std::mem;

use dope::{
    core::{
        Errno,
        driver::schedule::ready::wake::{fanout, table},
    },
    manifold::client::{close, conn},
    net::link::{egress::reserve::exact, pool::change::opening},
};
use o3::{
    alloc,
    buffer::{hold, write::consume::scope},
    depot::queue::resident::fifo,
};
use sark_core::manifold::client::io;
use sark_h2::conn::Build;

use crate::client::session;

#[derive(Debug)]
pub enum Error {
    Input(scope::Error),
    Retention(hold::Error),
    NotConnected,
    Exhausted(opening::Exhaustion),
    StaleCapability,
    Change(fanout::Error),
    Completion(table::Error),
    Sender(fifo::Reject),
    Disconnected(close::Reason),
    Transport(Errno),
    Transmit(conn::Transmit),
    Protocol(crate::Status),
    Create(session::Create<Build, alloc::Error>),
    Prepare(io::Error),
    Payload(io::Failure),
    Reserve(exact::Failure),
}

const _: () = {
    assert!(mem::size_of::<Option<Error>>() == mem::size_of::<Error>());
    assert!(mem::size_of::<Result<(), Error>>() == mem::size_of::<Error>());
    assert!(mem::size_of::<Error>() <= mem::size_of::<(crate::Status, u8)>());
};

impl From<crate::Status> for Error {
    fn from(status: crate::Status) -> Self {
        Self::Protocol(status)
    }
}
