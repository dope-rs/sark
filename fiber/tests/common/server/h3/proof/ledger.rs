use std::collections::BTreeMap;

use dope_quic::wire::stream::Id;
use o3::buffer::{hold, segments};
use sark_core::life::{Event, Outcome, audit};
use sark_http::owner::Change;

use crate::common::server::h3::proof::{ConnKey, Endpoint, Metric, charge};

#[derive(Debug)]
pub enum Error {
    Add(Metric, usize, usize),
    Remove(Metric, usize, usize),
    Charge(hold::Overflow),
    Terminal {
        previous: Outcome,
        incoming: Outcome,
    },
}

#[derive(Default)]
pub(super) struct Request {
    pub opened: usize,
    pub head_admitted: usize,
    pub scope_closed: usize,
    pub input_finished: usize,
}

pub(super) struct Credit {
    pub reserved_bytes: usize,
    pub reserved_chunks: segments::Count,
    pub owner_bytes: usize,
    pub owner_count: segments::Count,
}

impl Default for Credit {
    fn default() -> Self {
        Self {
            reserved_bytes: 0,
            reserved_chunks: segments::Count::EMPTY,
            owner_bytes: 0,
            owner_count: segments::Count::EMPTY,
        }
    }
}

#[derive(Default)]
pub(super) struct Tasks {
    pub task_admitted: usize,
    pub task_ready: usize,
    pub task_cancelled: usize,
}

#[derive(Default)]
pub(super) struct Response {
    pub staging: hold::Charge,
    pub transport: hold::Charge,
    pub handed: hold::Charge,
    pub linked: hold::Charge,
    pub materialized: usize,
}

#[derive(Default)]
pub(super) struct Entry {
    pub request: Request,
    pub credit: Credit,
    pub tasks: Tasks,
    pub response: Response,
    pub terminal: Option<Outcome>,
    pub retired: usize,
}

impl Entry {
    fn apply(&mut self, event: Event<ConnKey, Id>) -> Result<(), Error> {
        let ledger = self;
        match event {
            Event::PreAdmissionReserved { bytes, chunks, .. } => {
                let credit = &mut ledger.credit;
                credit.reserved_bytes = credit.reserved_bytes.checked_add(bytes).ok_or(
                    Error::Add(Metric::ReservedBytes, credit.reserved_bytes, bytes),
                )?;
                credit.reserved_chunks =
                    credit
                        .reserved_chunks
                        .checked_add(chunks)
                        .ok_or(Error::Add(
                            Metric::ReservedChunks,
                            credit.reserved_chunks.into_raw() as usize,
                            chunks.into_raw() as usize,
                        ))?;
            }
            Event::PreAdmissionReleased { bytes, chunks, .. } => {
                let credit = &mut ledger.credit;
                credit.reserved_bytes =
                    credit
                        .reserved_bytes
                        .checked_sub(bytes)
                        .ok_or(Error::Remove(
                            Metric::ReservedBytes,
                            credit.reserved_bytes,
                            bytes,
                        ))?;
                credit.reserved_chunks =
                    credit
                        .reserved_chunks
                        .checked_sub(chunks)
                        .ok_or(Error::Remove(
                            Metric::ReservedChunks,
                            credit.reserved_chunks.into_raw() as usize,
                            chunks.into_raw() as usize,
                        ))?;
            }
            Event::RequestOwnerAcquired { charge, .. } => {
                let credit = &mut ledger.credit;
                credit.owner_bytes =
                    credit
                        .owner_bytes
                        .checked_add(charge.bytes)
                        .ok_or(Error::Add(
                            Metric::OwnerBytes,
                            credit.owner_bytes,
                            charge.bytes,
                        ))?;
                credit.owner_count =
                    credit
                        .owner_count
                        .checked_add(charge.owners)
                        .ok_or(Error::Add(
                            Metric::OwnerCount,
                            credit.owner_count.into_raw() as usize,
                            charge.owners.into_raw() as usize,
                        ))?;
            }
            Event::ExchangeOpened(_) => ledger.request.opened += 1,
            Event::HeadAdmitted(_) => ledger.request.head_admitted += 1,
            Event::RequestOwnerScopeClosed(_) => {
                ledger.request.scope_closed += 1;
                ledger.credit.owner_bytes = 0;
                ledger.credit.owner_count = segments::Count::EMPTY;
            }
            Event::InputFinished(_) => ledger.request.input_finished += 1,
            Event::TaskAdmitted(_) => ledger.tasks.task_admitted += 1,
            Event::TaskPollReady(_) => ledger.tasks.task_ready += 1,
            Event::TaskCancelled(_) => ledger.tasks.task_cancelled += 1,
            Event::ResponseCommitted { charge, linked, .. } => {
                ledger.response.staging = ledger
                    .response
                    .staging
                    .try_add(charge)
                    .map_err(Error::Charge)?;
                ledger.response.linked = ledger
                    .response
                    .linked
                    .try_add(linked)
                    .map_err(Error::Charge)?;
            }
            Event::ResponseOwnership(effect) => match effect.change() {
                Change::StagingAcquired(_) => {
                    ledger.response.staging = ledger
                        .response
                        .staging
                        .try_add(effect.charge())
                        .map_err(Error::Charge)?
                }
                Change::Handoff => {
                    charge::remove(&mut ledger.response.staging, effect.charge())?;
                    ledger.response.transport = ledger
                        .response
                        .transport
                        .try_add(effect.charge())
                        .map_err(Error::Charge)?;
                    ledger.response.handed = ledger
                        .response
                        .handed
                        .try_add(effect.charge())
                        .map_err(Error::Charge)?;
                }
                Change::StagingReleased(_) => {
                    charge::remove(&mut ledger.response.staging, effect.charge())?
                }
                Change::TransportReleased(_) => {
                    charge::remove(&mut ledger.response.transport, effect.charge())?
                }
            },
            Event::ResponseBodyCopied { written, .. } => {
                let copied = &mut ledger.response.materialized;
                *copied = copied.checked_add(written).ok_or(Error::Add(
                    Metric::CopiedBytes,
                    *copied,
                    written,
                ))?;
            }
            Event::Terminal { outcome, .. } => {
                if let Some(previous) = ledger.terminal.replace(outcome) {
                    return Err(Error::Terminal {
                        previous,
                        incoming: outcome,
                    });
                }
            }
            Event::ExchangeRetired(_) => ledger.retired += 1,
            Event::SessionOpened(_)
            | Event::SessionEstablished(_)
            | Event::SessionClosing(_)
            | Event::SessionClosed(_)
            | Event::ExchangeDeadlineExpired { .. }
            | Event::BuildReady(_)
            | Event::TaskPollPending(_) => {}
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum Verification {
    Ledger(Error),
    Count {
        property: Metric,
        actual: usize,
        expected: usize,
    },
    ConnectionOwners,
    ReceiveBacking,
    EndpointClosure,
    MissingAudit,
    RepeatedHead,
    StagingOwners,
    TransportOwners,
    MissingTerminal,
    RepeatedTask,
    UnhandedOwner,
    NoExchanges,
    ExtraExchanges,
    NoRequests,
    RetainedScenario,
    ResponseError,
    MissingOracle,
    Fairness,

    Unsubmitted(usize),
    Missing {
        index: u32,
        generation: u32,
        stream: Id,
    },
}

pub struct Trace {
    pub events: Vec<Event<ConnKey, Id>>,
    pub(crate) audits: Vec<(ConnKey, audit::Snapshot)>,
}

impl Trace {
    pub(crate) fn verify(
        &self,
        endpoint: &Endpoint,
        expected: usize,
        inputs: impl IntoIterator<Item = (Option<(ConnKey, Id)>, bool)>,
    ) -> Result<(), Verification> {
        let Self { events, audits } = self;
        use sark_core::life::{Event, audit::Capability};

        let mut ledgers = BTreeMap::new();
        for &event in events {
            let exchange = match event {
                Event::ExchangeOpened(exchange)
                | Event::HeadAdmitted(exchange)
                | Event::RequestOwnerScopeClosed(exchange)
                | Event::InputFinished(exchange)
                | Event::BuildReady(exchange)
                | Event::TaskAdmitted(exchange)
                | Event::TaskPollPending(exchange)
                | Event::TaskPollReady(exchange)
                | Event::TaskCancelled(exchange)
                | Event::ExchangeRetired(exchange) => Some(exchange),
                Event::PreAdmissionReserved { exchange, .. }
                | Event::PreAdmissionReleased { exchange, .. }
                | Event::RequestOwnerAcquired { exchange, .. }
                | Event::ResponseCommitted { exchange, .. }
                | Event::ResponseBodyCopied { exchange, .. }
                | Event::Terminal { exchange, .. } => Some(exchange),
                Event::ResponseOwnership(effect) => Some(*effect.key()),
                _ => None,
            };
            if let Some(exchange) = exchange {
                let conn = exchange.connection();
                ledgers
                    .entry((conn.index, conn.generation, exchange.stream().get()))
                    .or_insert_with(Entry::default)
                    .apply(event)
                    .map_err(Verification::Ledger)?;
            }
        }
        let required = Capability::REQUEST_BOOK_ENTRIES
            .union(Capability::PRE_ADMISSION_BYTES)
            .union(Capability::PRE_ADMISSION_CHUNKS)
            .union(Capability::REQUEST_READY_NODES)
            .union(Capability::LIVE_TASKS)
            .union(Capability::REQUEST_OWNER_BYTES)
            .union(Capability::REQUEST_OWNERS)
            .union(Capability::CONNECTION_READY_ENTRIES)
            .union(Capability::H3_RESPONSE_OWNER_BYTES);
        for (_, snapshot) in audits {
            if !snapshot.is_zero(required) {
                return Err(Verification::ConnectionOwners);
            }
        }
        for (property, actual) in [
            (Metric::EndpointOwners, endpoint.owners),
            (Metric::EndpointBytes, endpoint.bytes),
        ] {
            if actual != 0 {
                return Err(Verification::Count {
                    property,
                    actual,
                    expected: 0,
                });
            }
        }
        for (valid, property) in [
            (
                endpoint.receive_quiescent == Some(true),
                Verification::ReceiveBacking,
            ),
            (
                endpoint.opened && endpoint.audited && endpoint.closed,
                Verification::EndpointClosure,
            ),
            (!audits.is_empty(), Verification::MissingAudit),
        ] {
            if !valid {
                return Err(property);
            }
        }
        let mut linked = 0;
        for ledger in ledgers.values() {
            for (property, actual, expected) in [
                (Metric::Opened, ledger.request.opened, 1),
                (Metric::ReservedBytes, ledger.credit.reserved_bytes, 0),
                (
                    Metric::ReservedChunks,
                    ledger.credit.reserved_chunks.into_raw() as usize,
                    0,
                ),
                (Metric::OwnerBytes, ledger.credit.owner_bytes, 0),
                (
                    Metric::OwnerCount,
                    ledger.credit.owner_count.into_raw() as usize,
                    0,
                ),
                (Metric::ScopeClosed, ledger.request.scope_closed, 1),
                (Metric::Retired, ledger.retired, 1),
                (
                    Metric::TaskEdges,
                    ledger.tasks.task_ready + ledger.tasks.task_cancelled,
                    ledger.tasks.task_admitted,
                ),
                (Metric::CopiedBytes, ledger.response.materialized, 0),
            ] {
                if actual != expected {
                    return Err(Verification::Count {
                        property,
                        actual,
                        expected,
                    });
                }
            }
            for (valid, property) in [
                (
                    ledger.request.head_admitted <= 1,
                    Verification::RepeatedHead,
                ),
                (
                    ledger.response.staging.is_empty(),
                    Verification::StagingOwners,
                ),
                (
                    ledger.response.transport.is_empty(),
                    Verification::TransportOwners,
                ),
                (ledger.terminal.is_some(), Verification::MissingTerminal),
                (
                    ledger.tasks.task_admitted <= 1
                        && ledger.tasks.task_ready <= 1
                        && ledger.tasks.task_cancelled <= 1,
                    Verification::RepeatedTask,
                ),
                (
                    ledger.response.linked.logical_bytes <= ledger.response.handed.logical_bytes
                        && ledger.response.linked.resident_bytes
                            <= ledger.response.handed.resident_bytes
                        && ledger.response.linked.owners <= ledger.response.handed.owners,
                    Verification::UnhandedOwner,
                ),
            ] {
                if !valid {
                    return Err(property);
                }
            }
            linked += usize::from(!ledger.response.linked.is_empty());
        }
        if linked != expected {
            return Err(Verification::Count {
                property: Metric::Linked,
                actual: linked,
                expected,
            });
        }
        let mut checked = false;
        for (index, (target, finished)) in inputs.into_iter().enumerate() {
            let (conn, stream) = target.ok_or(Verification::Unsubmitted(index))?;
            let ledger = ledgers
                .remove(&(conn.index, conn.generation, stream.get()))
                .ok_or(Verification::Missing {
                    index: conn.index,
                    generation: conn.generation,
                    stream,
                })?;
            let expected = usize::from(finished);
            if ledger.request.input_finished != expected {
                return Err(Verification::Count {
                    property: Metric::InputFinished,
                    actual: ledger.request.input_finished,
                    expected,
                });
            }
            checked = true;
        }
        if !checked {
            return Err(Verification::NoExchanges);
        }
        if !ledgers.is_empty() {
            return Err(Verification::ExtraExchanges);
        }
        Ok(())
    }
}
