use std::{iter, num};

use dope_fiber::task::time::sleep;
use sark_h3::{conn, conn::options, manifold};
use sark_http::field;
mod charge;
pub mod ledger;
pub mod request;
use std::{cell, cell::Cell, net, rc, rc::Rc, time::Instant};

use dope::{
    core::{driver::result::SubmitError, io::datagram::ReceiveError},
    manifold::network::datagram::RejectCause,
    runtime::shutdown,
};
use dope_quic::{
    conn::{id, life::tls::clients::Authority, output::transmit, stream},
    errors,
    proto::param,
    wire::stream::Id,
};
use dope_test::harness::observe::VecCellExt as _;
use ledger::Trace;
use sark_core::manifold::stream::Boundary;

use crate::common::server::h3::proof::request::Spec;

pub(super) type Result<T> = std::result::Result<T, Failure>;

pub(crate) const LISTENER: u8 = 17;

pub(crate) const DIALER: u8 = 18;

pub const RAW_INPUT: &[u8] = b"raw-owner-across-h3-wake";

pub const JSON_INPUT: &[u8] = br#"{"value":"json-owner-across-h3-wake"}"#;

pub const NESTED_JSON_INPUT: &[u8] = br#"{"items":[{"id":1,"value":"first-owner","tags":["nested","owner"]},{"id":2,"value":"second-owner","tags":["nested","owner"]}],"featured":{"id":3,"value":"featured-owner","tags":["nested","owner"]}}"#;

pub(crate) const LARGE_JSON_LEN: usize = 64 * 1024;

pub const LARGE_JSON_INPUT: &[u8; LARGE_JSON_LEN] = &large_json_input();

pub const SEGMENTED_FIRST: &[u8] = b"first-segment";

pub(crate) const SEGMENTED_SECOND: &[u8] = b"second-segment";

pub const SEGMENTED_INPUT: &[u8] = b"first-segmentsecond-segment";

const fn large_json_input() -> [u8; LARGE_JSON_LEN] {
    let mut input = [b' '; LARGE_JSON_LEN];
    let head = b"{\"value\":1}";
    let mut index = 0;
    while index < head.len() {
        input[index] = head[index];
        index += 1;
    }
    input
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Dispatch {
    Initial,
    AfterCancellation,
    AfterFailure,
    AfterGoAway,
}

pub(crate) struct Connect {
    pub(crate) slot: usize,
    pub(crate) peer: net::SocketAddr,
    pub(crate) authority: Authority,
    pub(crate) params: param::Set,
    pub(crate) initial_dcid: Vec<u8>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CancelAction {
    RequestCancelled,
    StopSending,
    Qpack,
    KeyClose,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Readiness {
    ExchangeOpened,
    HeadAdmitted,
    TaskPollPending,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Cancellation {
    pub action: CancelAction,
    pub readiness: Readiness,
    pub expected_task_drops: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnKey {
    pub(crate) index: u32,
    pub(crate) generation: u32,
}

pub(crate) fn connection_key<I: Copy, const ID: u8>(handle: id::Handle<I, ID>) -> ConnKey {
    ConnKey {
        index: handle.index(),
        generation: handle.generation(),
    }
}

pub struct Proof<'a> {
    pub(crate) requests: Vec<Spec<'a>>,
    pub replies: Replies,
    pub trace: Trace,
    pub cancel: Cancel,
    pub network: Network,
}

/// Keeps the coupled proof borrowed while failure reporting stays independent.
pub(crate) struct Shared<'a> {
    proof: cell::RefCell<Proof<'a>>,
    pub(crate) failures: Rc<Failures>,
}

impl<'a> Shared<'a> {
    pub(crate) fn new(proof: Proof<'a>) -> Self {
        Self {
            failures: Rc::clone(&proof.network.failures),
            proof: cell::RefCell::new(proof),
        }
    }

    pub(crate) fn read(&self) -> Result<cell::Ref<'_, Proof<'a>>> {
        self.proof.try_borrow().map_err(Failure::Borrow)
    }

    pub(crate) fn write(&self) -> Result<cell::RefMut<'_, Proof<'a>>> {
        self.proof.try_borrow_mut().map_err(Failure::BorrowMut)
    }

    pub(crate) fn into_inner(self) -> Proof<'a> {
        self.proof.into_inner()
    }

    pub(crate) fn fail(&self, error: Failure) {
        self.failures.fail(error);
    }

    pub(crate) fn reject(&self) {
        match self.write() {
            Ok(mut proof) => proof.network.reject(),
            Err(error) => self.fail(error),
        }
    }
}

pub(crate) struct Failures {
    pub(crate) values: cell::Cell<Vec<Failure>>,
    stop: cell::Cell<Option<shutdown::Trigger>>,
}

impl Failures {
    fn new(stop: shutdown::Trigger) -> Self {
        Self {
            values: Cell::new(Vec::new()),
            stop: Cell::new(Some(stop)),
        }
    }

    pub(crate) fn record(&self, error: Failure) {
        self.values.push(error);
    }

    fn fail(&self, error: Failure) {
        self.record(error);
        self.stop();
    }

    fn stop(&self) {
        if let Some(stop) = self.stop.take()
            && let Err(error) = stop.fire()
        {
            self.record(Failure::Shutdown(error));
        }
    }

    pub(crate) fn is_pending(&self) -> bool {
        let stop = self.stop.take();
        let pending = stop.is_some();
        self.stop.set(stop);
        pending
    }
}

pub struct Replies {
    pub streams: Vec<Option<(ConnKey, Id)>>,
    pub(crate) bodies: Vec<Vec<u8>>,
    pub(crate) tails_sent: Vec<bool>,
    pub(crate) status_ok: Vec<bool>,
    pub(crate) error_ok: Vec<bool>,
    pub(crate) finished: Vec<bool>,
    pub completion_order: Vec<usize>,
}

pub struct Cancel {
    pub(crate) request: Option<Cancellation>,
    pub(crate) ready: bool,
    pub issued: bool,
    pub retired: bool,
    pub followup: bool,
    pub dropped: rc::Rc<cell::Cell<usize>>,
}

pub struct Network {
    pub goaway: Option<u64>,
    pub(crate) connect: Vec<Connect>,
    pub(crate) endpoint: Endpoint,
    pub(crate) failures: Rc<Failures>,
    pub(crate) rejected: usize,
    pub(crate) deadline: Instant,
}

#[derive(Debug, Default)]
pub(crate) struct Endpoint {
    pub(crate) owners: usize,
    pub(crate) bytes: usize,
    pub(crate) receive_quiescent: Option<bool>,
    pub(crate) opened: bool,
    pub(crate) audited: bool,
    pub(crate) closed: bool,
}

impl<'a> Proof<'a> {
    pub(crate) fn new(
        requests: Vec<Spec<'a>>,
        deadline: Instant,
        stop: shutdown::Trigger,
        cancel: Option<Cancellation>,
    ) -> Self {
        let count = requests.len();
        Self {
            requests,
            replies: Replies {
                streams: vec![None; count],
                bodies: iter::repeat_with(Vec::new).take(count).collect(),
                tails_sent: vec![false; count],
                status_ok: vec![false; count],
                error_ok: vec![false; count],
                finished: vec![false; count],
                completion_order: Vec::with_capacity(count),
            },
            trace: Trace {
                events: Vec::new(),
                audits: Vec::new(),
            },
            cancel: Cancel {
                request: cancel,
                ready: false,
                issued: false,
                retired: false,
                followup: false,
                dropped: Rc::new(Cell::new(0)),
            },
            network: Network {
                goaway: None,
                connect: Vec::new(),
                endpoint: Endpoint::default(),
                failures: Rc::new(Failures::new(stop)),
                rejected: 0,
                deadline,
            },
        }
    }

    pub(crate) fn slot(&self, conn: ConnKey, stream: Id) -> Option<usize> {
        self.replies
            .streams
            .iter()
            .position(|item| *item == Some((conn, stream)))
    }

    pub(crate) fn tail(&mut self, slot: usize) -> Result<Option<(Id, &'a [u8], Boundary)>> {
        let request = self.requests[slot];
        if !request.data.defer_tail || self.replies.tails_sent[slot] {
            return Ok(None);
        }
        let Some((split, (_, stream))) = request.data.split.zip(self.replies.streams[slot]) else {
            return Ok(None);
        };
        let bytes = request.data.bytes.get(split..).ok_or(Failure::Split {
            length: request.data.bytes.len(),
            at: split,
        })?;
        self.replies.tails_sent[slot] = true;
        Ok(Some((stream, bytes, request.data.boundary)))
    }

    pub(crate) fn stop_done(&mut self) {
        if self.replies.finished.iter().all(|finished| *finished) {
            self.network.failures.stop();
        }
    }

    pub(crate) fn fail(&mut self, error: Failure) {
        self.network.failures.fail(error);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancel.request.is_some_and(|cancel| {
            self.cancel.issued
                && self.cancel.retired
                && self.cancel.dropped.get() == cancel.expected_task_drops
        })
    }

    pub(crate) fn has_failed(&self) -> bool {
        let failures = self
            .requests
            .iter()
            .enumerate()
            .filter_map(|(slot, request)| {
                (request.dispatch == Dispatch::Initial && request.expect.error.is_some())
                    .then_some(slot)
            });
        let mut found = false;
        for slot in failures {
            found = true;
            if !self.replies.error_ok[slot] || !self.replies.finished[slot] {
                return false;
            }
        }
        found
    }
}

impl Network {
    pub(crate) fn reject(&mut self) {
        match self.rejected.checked_add(1) {
            Some(count) => self.rejected = count,
            None => self
                .failures
                .record(Failure::Counter(Metric::RejectedPeers, self.rejected, 1)),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Metric {
    LogicalBytes,
    Owners,
    ResidentBytes,
    ReservedBytes,
    ReservedChunks,
    OwnerBytes,
    OwnerCount,
    CopiedBytes,
    EndpointOwners,
    EndpointBytes,
    Opened,
    ScopeClosed,
    Retired,
    TaskEdges,
    Linked,
    InputFinished,
    Oracles,
    RejectedPeers,
    RequestSlots,
    OutgoingBytes,
    Segments,
    NestedItems,
    ConnectionOrder,
    OwnerAcquire,
    BytesAcquire,
    OwnerRelease,
    BytesRelease,
}

#[derive(Debug)]
pub enum Failure {
    Borrow(cell::BorrowError),
    BorrowMut(cell::BorrowMutError),
    Field(field::Error),
    Scenario(String),
    Receive(manifold::Error),
    Socket(ReceiveError),
    Transmit(transmit::Error),
    Rejected(RejectCause),
    Submit(SubmitError),
    Flush(manifold::Error),
    Send(conn::Error),
    Session(options::Error),
    Connect(errors::Connect),
    Access(errors::Access),
    Stream(stream::Error),
    Index(num::TryFromIntError),
    Shutdown(shutdown::FireError),
    Counter(Metric, usize, usize),
    Sleep(sleep::Error),
    Guard {
        method: Vec<u8>,
        path: Option<Vec<u8>>,
    },
    Segments([Option<Vec<u8>>; 2]),
    Owner {
        before: usize,
        after: usize,
    },
    Value(u64),
    Coalesced,
    SavedOccupied,
    SavedMissing,
    DuplexInput,
    DuplexFinish,
    DuplexActivate,
    Tag(Option<Vec<u8>>),
    Route {
        team: Vec<u8>,
        user: Vec<u8>,
    },
    Split {
        length: usize,
        at: usize,
    },
}

impl From<field::Error> for Failure {
    fn from(error: field::Error) -> Self {
        Self::Field(error)
    }
}
