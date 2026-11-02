pub(super) mod driver;
pub(super) mod ingress;
pub(super) mod phase;
mod raw;
mod table;

use o3::{
    alloc,
    buffer::resident,
    depot::{capacity, queue::indexed::lanes::typed::Value},
};
use sark_core::{
    body::credit,
    decode::{self, exec, request},
    manifold::schedule::typed,
};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{self, event, machine},
    frame::numbers,
    server::{payload::Buffer, response, state::exchange::Routed},
    stream,
};

type Delayed<'d, R> =
    <<<R as decode::Decode>::Head as request::Head>::Start as request::Selection<
        <R as decode::Decode>::Prepared,
    >>::Deferred<Routed<'d, <R as decode::Decode>::Prepared, <R as decode::Decode>::Routes>>;

type Receiving<'d, R> = request::Admitted<exec::Sync, <R as decode::Decode>::Prepared, Buffer<'d>>;

type State<'d, R> = Value<phase::Phase<'d, R>, response::Pending<'d>, Receiving<'d, R>>;

type Next<'d, R> =
    typed::Next<phase::Phase<'d, R>, response::Pending<'d>, Receiving<'d, R>, conn::DataBlock>;

type Control<'d, R> = typed::Control<
    phase::Phase<'d, R>,
    response::Pending<'d>,
    Receiving<'d, R>,
    conn::DataBlock,
    numbers::ErrorCode,
>;

/// Per-connection synchronous exchange book.
pub struct Book<'d, R: decode::Decode> {
    pub(in crate::server::sync) runtime: typed::Runtime<
        stream::Key,
        phase::Phase<'d, R>,
        response::Pending<'d>,
        Receiving<'d, R>,
        conn::DataBlock,
    >,
    credit: credit::raw::Account,
}

impl<'d, R: decode::Decode> Book<'d, R> {
    pub(super) fn try_new(cap: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self {
            runtime: typed::Runtime::try_bounded(cap, cap)?,
            credit: credit::raw::Account::new(),
        })
    }

    pub(super) fn has_ready(&self) -> bool {
        self.runtime.has_runnable()
    }

    pub(super) fn has_events(
        conn: &mut machine::Connection<
            crate::ServerRole,
            R::Plan,
            resident::Shared<'d>,
            rules::RejectPush,
            event::Ignore,
        >,
    ) -> bool {
        conn.ingress().has_event()
    }

    pub(super) fn has_input(
        conn: &mut machine::Connection<
            crate::ServerRole,
            R::Plan,
            resident::Shared<'d>,
            rules::RejectPush,
            event::Ignore,
        >,
    ) -> bool {
        Self::has_events(conn) || conn.ingress().is_blocked()
    }
}
