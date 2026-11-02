use std::{cell, fmt, net::SocketAddr, rc};

use dope::core::driver::storage;
use dope_quic::{
    conn::{
        id,
        life::options::{Options, builder},
    },
    proto::param,
    runtime::mux::handler::{hook::wire::packet, net},
    wire::stream::Id,
};
use sark_core::{
    life,
    life::{audit, endpoint, exchange},
};
use sark_h3::{
    conn::{
        options,
        options::{Protocol, plan, plan::Config},
    },
    manifold::server::failure,
};
use sark_http::field::metadata::block::Connect;

use crate::common::server::h3::{
    apps::{FAIR_REQUESTS, State},
    proof::{ConnKey, Failure, Failures, LISTENER, Metric, Readiness, Shared, connection_key},
};

pub(crate) struct Storage<'d> {
    pub(crate) state: State<'d>,
}

pub(crate) struct StoragePlan {
    pub(crate) cancel_dropped: rc::Rc<cell::Cell<usize>>,
    pub(crate) errors: rc::Rc<Failures>,
}

impl storage::Plan for StoragePlan {
    type Output<'p, 'd>
        = Storage<'d>
    where
        Self: 'd,
        'd: 'p;
}

impl storage::Claimless for StoragePlan {}

impl storage::Direct for StoragePlan {
    fn build<'p, 'd>(self) -> Self::Output<'p, 'd>
    where
        Self: 'd,
        'd: 'p,
    {
        Storage {
            state: State {
                admitted: cell::Cell::new(0),
                waiters: cell::Cell::new(Vec::with_capacity(FAIR_REQUESTS)),
                saved: cell::Cell::new(None),
                cancel_dropped: self.cancel_dropped,
                errors: self.errors,
            },
        }
    }
}

#[derive(Clone)]
pub(crate) struct Probe<'a, D>(pub(crate) rc::Rc<Shared<'a>>, pub(crate) D);

impl<D> failure::Packet for Probe<'_, D> {
    fn packet(&mut self, from: SocketAddr, len: usize, error: packet::Failure<'_>) {
        self.0.fail(Failure::Scenario(format!(
            "E2E QUIC packet from {from}, {len} bytes: {error:?}",
        )));
    }
}

impl<C, D> failure::Sink<C> for Probe<'_, D> {
    fn failure(&mut self, event: failure::Event<'_, C>) {
        match event {
            failure::Event::Create { error, .. } => self
                .0
                .fail(Failure::Scenario(format!("E2E H3 setup: {error:?}"))),
            failure::Event::Certificate(_) => self.0.reject(),
            failure::Event::Transmit { error, .. } => self.0.fail(Failure::Transmit(error)),
            failure::Event::Rejected(error) => self.0.fail(Failure::Rejected(error)),
            failure::Event::Receive(error) => self.0.fail(Failure::Socket(error)),
            failure::Event::Submit(error) => self.0.fail(Failure::Submit(error)),
        }
    }
}

pub(crate) fn project_exchange<I: Copy, const ID: u8>(
    exchange: exchange::Id<id::Handle<I, ID>, Id>,
) -> exchange::Id<ConnKey, Id> {
    exchange::Id::new(connection_key(exchange.connection()), exchange.stream())
}

pub(crate) fn project_event<I: Copy, const ID: u8>(
    event: life::Event<id::Handle<I, ID>, Id>,
) -> life::Event<ConnKey, Id> {
    use sark_core::life::Event;

    match event {
        Event::SessionOpened(conn) => Event::SessionOpened(connection_key(conn)),
        Event::SessionEstablished(conn) => Event::SessionEstablished(connection_key(conn)),
        Event::ExchangeOpened(exchange) => Event::ExchangeOpened(project_exchange(exchange)),
        Event::HeadAdmitted(exchange) => Event::HeadAdmitted(project_exchange(exchange)),
        Event::PreAdmissionReserved {
            exchange,
            bytes,
            chunks,
        } => Event::PreAdmissionReserved {
            exchange: project_exchange(exchange),
            bytes,
            chunks,
        },
        Event::PreAdmissionReleased {
            exchange,
            bytes,
            chunks,
        } => Event::PreAdmissionReleased {
            exchange: project_exchange(exchange),
            bytes,
            chunks,
        },
        Event::RequestOwnerAcquired { exchange, charge } => Event::RequestOwnerAcquired {
            exchange: project_exchange(exchange),
            charge,
        },
        Event::RequestOwnerScopeClosed(exchange) => {
            Event::RequestOwnerScopeClosed(project_exchange(exchange))
        }
        Event::InputFinished(exchange) => Event::InputFinished(project_exchange(exchange)),
        Event::BuildReady(exchange) => Event::BuildReady(project_exchange(exchange)),
        Event::TaskAdmitted(exchange) => Event::TaskAdmitted(project_exchange(exchange)),
        Event::TaskPollPending(exchange) => Event::TaskPollPending(project_exchange(exchange)),
        Event::TaskPollReady(exchange) => Event::TaskPollReady(project_exchange(exchange)),
        Event::TaskCancelled(exchange) => Event::TaskCancelled(project_exchange(exchange)),
        Event::ResponseCommitted {
            exchange,
            body_bytes: bytes,
            charge,
            linked,
        } => Event::ResponseCommitted {
            exchange: project_exchange(exchange),
            body_bytes: bytes,
            charge,
            linked,
        },
        Event::ResponseBodyCopied {
            exchange,
            read,
            written,
            cause,
        } => Event::ResponseBodyCopied {
            exchange: project_exchange(exchange),
            read,
            written,
            cause,
        },
        Event::ResponseOwnership(effect) => {
            Event::ResponseOwnership(effect.map_key(project_exchange))
        }
        Event::Terminal { exchange, outcome } => Event::Terminal {
            exchange: project_exchange(exchange),
            outcome,
        },
        Event::ExchangeDeadlineExpired { exchange, phase } => Event::ExchangeDeadlineExpired {
            exchange: project_exchange(exchange),
            phase,
        },
        Event::ExchangeRetired(exchange) => Event::ExchangeRetired(project_exchange(exchange)),
        Event::SessionClosing(conn) => Event::SessionClosing(connection_key(conn)),
        Event::SessionClosed(conn) => Event::SessionClosed(connection_key(conn)),
    }
}

impl<'d, D: Fn(&dyn fmt::Debug)> life::Observer<id::Handle<id::Driver<'d>, LISTENER>, Id>
    for Probe<'_, D>
{
    fn observe(&mut self, event: life::Event<id::Handle<id::Driver<'d>, LISTENER>, Id>) {
        let mut proof = match self.0.write() {
            Ok(proof) => proof,
            Err(error) => return self.0.fail(error),
        };
        let ready = proof.cancel.request.is_some_and(|cancel| {
            matches!(
                (cancel.readiness, event),
                (Readiness::ExchangeOpened, life::Event::ExchangeOpened(_))
                    | (Readiness::HeadAdmitted, life::Event::HeadAdmitted(_))
                    | (Readiness::TaskPollPending, life::Event::TaskPollPending(_))
            )
        });
        if ready {
            proof.cancel.ready = true;
        }
        if let life::Event::ExchangeRetired(exchange) = event
            && proof.cancel.issued
            && proof
                .replies
                .streams
                .first()
                .copied()
                .flatten()
                .is_some_and(|(conn, stream)| {
                    connection_key(exchange.connection()) == conn && exchange.stream() == stream
                })
        {
            proof.cancel.retired = true;
        }
        if let life::Event::SessionClosing(conn) = event {
            let conn = connection_key(conn);
            for slot in 0..proof.requests.len() {
                if proof.requests[slot].expect.connection_closed
                    && proof.replies.streams[slot].is_some_and(|(owner, _)| owner == conn)
                {
                    proof.replies.error_ok[slot] = true;
                    proof.replies.finished[slot] = true;
                }
            }
            proof.stop_done();
        }
        proof.trace.events.push(project_event(event));
    }

    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E) {
        self.1(cause);
    }

    fn audit(&mut self, conn: id::Handle<id::Driver<'d>, LISTENER>, snapshot: audit::Snapshot) {
        let mut proof = match self.0.write() {
            Ok(proof) => proof,
            Err(error) => return self.0.fail(error),
        };
        proof.trace.audits.push((connection_key(conn), snapshot));
    }
}

impl<'d, D>
    endpoint::Observer<id::Domain<id::Driver<'d>, LISTENER>, net::Owner<id::Driver<'d>, LISTENER>>
    for Probe<'_, D>
{
    fn observe(
        &mut self,
        event: endpoint::Event<
            id::Domain<id::Driver<'d>, LISTENER>,
            net::Owner<id::Driver<'d>, LISTENER>,
        >,
    ) {
        use endpoint::Event;

        let mut proof = match self.0.write() {
            Ok(proof) => proof,
            Err(error) => return self.0.fail(error),
        };
        match event {
            Event::Opened(_) => proof.network.endpoint.opened = true,
            Event::Receive { error, .. } => {
                proof.fail(Failure::Scenario(format!("UDP receive failed: {error:?}")))
            }
            Event::Accepted { bytes, .. } => {
                let endpoint = &mut proof.network.endpoint;
                let Some(owners) = endpoint.owners.checked_add(1) else {
                    let count = endpoint.owners;
                    return proof.fail(Failure::Counter(Metric::OwnerAcquire, count, 1));
                };
                let Some(total) = endpoint.bytes.checked_add(bytes) else {
                    let count = endpoint.bytes;
                    return proof.fail(Failure::Counter(Metric::BytesAcquire, count, bytes));
                };
                endpoint.owners = owners;
                endpoint.bytes = total;
            }
            Event::Transferred { .. } => {}
            Event::Settled { bytes, .. } => {
                let endpoint = &mut proof.network.endpoint;
                let Some(owners) = endpoint.owners.checked_sub(1) else {
                    let count = endpoint.owners;
                    return proof.fail(Failure::Counter(Metric::OwnerRelease, count, 1));
                };
                let Some(total) = endpoint.bytes.checked_sub(bytes) else {
                    let count = endpoint.bytes;
                    return proof.fail(Failure::Counter(Metric::BytesRelease, count, bytes));
                };
                endpoint.owners = owners;
                endpoint.bytes = total;
            }
            Event::Audit { snapshot, .. } => {
                proof.network.endpoint.receive_quiescent = snapshot
                    .receive_resident_bytes
                    .map(|bytes| bytes.into_raw() == 0);
                proof.network.endpoint.audited = true;
            }
            Event::Closed(_) => proof.network.endpoint.closed = true,
        }
    }
}

#[derive(Debug)]
pub enum ConfigError {
    Params(param::Error),
    Options(builder::Invalid),
}

pub(crate) fn config_with_streams(streams: u64) -> Result<Options, ConfigError> {
    let params = param::Set::builder()
        .idle_ms(30_000)
        .initial_data(1 << 20)
        .local_bidi(1 << 20)
        .remote_bidi(1 << 20)
        .stream_uni(1 << 20)
        .bidi_count(streams)
        .uni_count(8)
        .try_build()
        .map_err(ConfigError::Params)?;
    Options::builder()
        .transport(|path| path.params(params))
        .try_build()
        .map_err(ConfigError::Options)
}

pub(crate) fn protocol() -> Result<plan::Config, options::Error> {
    Config::new(options::Config {
        protocol: Protocol {
            extended_connect: Connect::Enabled,
            ..Protocol::default()
        },
        ..options::Config::default()
    })
}
