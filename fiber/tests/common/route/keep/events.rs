use std::{
    cell::Cell,
    fmt,
    fs::File,
    io::{self, Write as _},
    os::fd::AsFd as _,
};

use dope::{core::driver::route, manifold::network::listener::conn};
use dope_test::harness::{observe::VecCellExt as _, signals::LocalGateRef};
use o3::buffer::hold;
use sark_core::life::{self, audit};

pub fn diagnose(cause: &dyn fmt::Debug) {
    write(format_args!("lifecycle diagnostic: {cause:?}"));
}

pub fn diagnose_h1(cause: &dyn fmt::Debug) {
    write(format_args!("H1 lifecycle diagnostic: {cause:?}"));
}

fn write(message: fmt::Arguments<'_>) {
    let mut stderr = File::from(
        io::stderr()
            .as_fd()
            .try_clone_to_owned()
            .expect("diagnostic descriptor"),
    );
    writeln!(stderr, "{message}").expect("diagnostic output");
}

#[derive(Clone, Copy)]
pub struct Log<'a, 'name, D>(
    pub &'a Cell<Vec<&'name str>>,
    pub &'a Cell<Option<audit::Snapshot>>,
    pub D,
);

type Counts = (usize, u32);

pub struct Reservation<'a, O> {
    observer: O,
    ready: LocalGateRef<'a>,
    closed: LocalGateRef<'a>,
    reserved: Counts,
    released: Counts,
    failed: &'a Cell<Option<(Counts, Counts)>>,
}

impl<'a, O> Reservation<'a, O> {
    pub fn new(
        observer: O,
        ready: LocalGateRef<'a>,
        closed: LocalGateRef<'a>,
        failed: &'a Cell<Option<(Counts, Counts)>>,
    ) -> Self {
        Self {
            observer,
            ready,
            closed,
            reserved: (0, 0),
            released: (0, 0),
            failed,
        }
    }
}

impl<C: Copy, S: Copy, O: life::Observer<C, S>> life::Observer<C, S> for Reservation<'_, O> {
    fn observe(&mut self, event: life::Event<C, S>) {
        let reserved = matches!(&event, life::Event::PreAdmissionReserved { .. });
        match &event {
            life::Event::PreAdmissionReserved { bytes, chunks, .. } => {
                self.reserved.0 += bytes;
                self.reserved.1 += chunks.into_raw();
            }
            life::Event::PreAdmissionReleased { bytes, chunks, .. } => {
                self.released.0 += bytes;
                self.released.1 += chunks.into_raw();
            }
            life::Event::ExchangeRetired(_) => {
                if self.reserved != self.released && self.failed.get().is_none() {
                    self.failed.set(Some((self.reserved, self.released)));
                }
            }
            life::Event::SessionClosed(_) => self.closed.hit(),
            _ => {}
        }
        self.observer.observe(event);
        if reserved {
            if (self.reserved.0 == 0 || self.released != (0, 0)) && self.failed.get().is_none() {
                self.failed.set(Some((self.reserved, self.released)));
            }
            self.ready.hit();
        }
    }

    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E) {
        self.observer.diagnostic(cause);
    }

    fn audit(&mut self, conn: C, state: audit::Snapshot) {
        self.observer.audit(conn, state);
    }
}

/// Fills an ingress page so the unfinished request owns pre-admission credit.
pub const PENDING: [&[u8]; 3] = [
    b"POST /body HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n4000\r\n",
    &[b'a'; 16 * 1024],
    b"\r\n",
];

pub const MALFORMED_CHUNK: &[u8] = b"z\r\n";

pub const REJECTED_REQUESTS: [(&[u8], &[u8]); 6] = [
    (b"GET /ping HTTP/1.1\r", b"X\r\nHost: local\r\n\r\n"),
    (b"GET /ping H\r", b"X"),
    (
        b"GET /ping HTTP/1.1\r\nHost: local\r\nContent-Length: ",
        b"invalid\r\n\r\n",
    ),
    (b"GET /ping HTTP/1.1\r\nX-Trace: pending", b"\r\n\r\n"),
    (
        b"GET /ping HTTP/1.1\r\nHost: local\r\nContent-Length: ",
        b"1\r\n\r\nx",
    ),
    (
        b"POST /body HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n",
        MALFORMED_CHUNK,
    ),
];

pub const OWNERS: audit::Capability = audit::Capability::PRE_ADMISSION_BYTES
    .union(audit::Capability::PRE_ADMISSION_CHUNKS)
    .union(audit::Capability::REQUEST_OWNERS)
    .union(audit::Capability::TRANSPORT_RESPONSE_OWNER_BYTES)
    .union(audit::Capability::RESPONSE_STAGING_OWNER_BYTES);

fn capture(state: audit::Snapshot, failed: &Cell<Option<audit::Snapshot>>) {
    if !state.is_zero(OWNERS) && failed.get().is_none() {
        failed.set(Some(state));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Identity {
    Opened(u64),
    Committed(u64),
    Handoff(u64, hold::Charge),
    StagingReleased(u64),
    Released(u64, hold::Charge),
    Retired(u64),
    Closed,
}

impl Identity {
    pub fn select<T>(events: &[Identity], op: impl FnMut(Identity) -> Option<T>) -> Vec<T> {
        events.iter().copied().filter_map(op).collect()
    }
}

#[derive(Clone, Copy)]
pub struct IdentityLog<'a, D>(
    pub &'a Cell<Vec<Identity>>,
    pub &'a Cell<Option<audit::Snapshot>>,
    pub D,
);

impl<'d, ID: route::Identity, D: Fn(&dyn fmt::Debug)> life::Observer<conn::Id<'d, ID>, u64>
    for IdentityLog<'_, D>
{
    fn observe(&mut self, event: life::Event<conn::Id<'d, ID>, u64>) {
        use sark_core::life::Event;
        use sark_http::owner::{Change, cause};

        let fact = match event {
            Event::ExchangeOpened(exchange) => Identity::Opened(exchange.stream()),
            Event::ResponseCommitted { exchange, .. } => Identity::Committed(exchange.stream()),
            Event::ResponseOwnership(effect) => match effect.change() {
                Change::StagingAcquired(
                    cause::Acquire::Committed | cause::Acquire::Split | cause::Acquire::StreamChunk,
                ) => return,
                Change::Handoff => Identity::Handoff(effect.key().stream(), effect.charge()),
                Change::StagingReleased(
                    cause::Staging::TransportAccepted
                    | cause::Staging::Buffered
                    | cause::Staging::Reset
                    | cause::Staging::Closed
                    | cause::Staging::PeerRetired,
                ) => Identity::StagingReleased(effect.key().stream()),
                Change::TransportReleased(
                    cause::Transport::Sent
                    | cause::Transport::Acked
                    | cause::Transport::Stopped
                    | cause::Transport::Reset
                    | cause::Transport::Closed
                    | cause::Transport::PeerRetired,
                ) => Identity::Released(effect.key().stream(), effect.charge()),
            },
            Event::ExchangeRetired(exchange) => Identity::Retired(exchange.stream()),
            Event::SessionClosed(_) => Identity::Closed,
            _ => return,
        };
        self.0.push(fact);
    }

    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E) {
        (self.2)(cause);
    }

    fn audit(&mut self, _: conn::Id<'d, ID>, state: audit::Snapshot) {
        capture(state, self.1);
    }
}

impl<'d, ID: route::Identity, D: Fn(&dyn fmt::Debug)> life::Observer<conn::Id<'d, ID>, u64>
    for Log<'_, '_, D>
{
    fn observe(&mut self, event: life::Event<conn::Id<'d, ID>, u64>) {
        use sark_http::owner::{Change, cause};

        let name = match event {
            life::Event::SessionOpened(_) => "session-opened",
            life::Event::SessionEstablished(_) => "session-established",
            life::Event::ExchangeOpened(_) => "exchange-opened",
            life::Event::HeadAdmitted(_) => "head-admitted",
            life::Event::PreAdmissionReserved { .. } => "pre-admission-reserved",
            life::Event::PreAdmissionReleased { .. } => "pre-admission-released",
            life::Event::RequestOwnerAcquired { .. } => "request-owner-acquired",
            life::Event::RequestOwnerScopeClosed(_) => "request-owner-scope-closed",
            life::Event::InputFinished(_) => "input-finished",
            life::Event::ResponseCommitted { .. } => "response-committed",
            life::Event::ResponseOwnership(effect) => match effect.change() {
                Change::StagingAcquired(
                    cause::Acquire::Committed | cause::Acquire::Split | cause::Acquire::StreamChunk,
                ) => return,
                Change::Handoff => "response-handoff",
                Change::StagingReleased(
                    cause::Staging::TransportAccepted
                    | cause::Staging::Buffered
                    | cause::Staging::Reset
                    | cause::Staging::Closed
                    | cause::Staging::PeerRetired,
                ) => return,
                Change::TransportReleased(
                    cause::Transport::Sent
                    | cause::Transport::Acked
                    | cause::Transport::Stopped
                    | cause::Transport::Reset
                    | cause::Transport::Closed
                    | cause::Transport::PeerRetired,
                ) => "response-transport-released",
            },
            life::Event::Terminal { .. } => "terminal",
            life::Event::ExchangeRetired(_) => "exchange-retired",
            life::Event::SessionClosed(_) => "session-closed",
            _ => return,
        };
        self.0.push(name);
    }

    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E) {
        (self.2)(cause);
    }

    fn audit(&mut self, _: conn::Id<'d, ID>, state: audit::Snapshot) {
        capture(state, self.1);
    }
}
