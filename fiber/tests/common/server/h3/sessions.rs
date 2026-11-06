use std::{
    cell::Cell,
    convert::Infallible,
    fmt, io, mem,
    net::{SocketAddr, UdpSocket},
    num,
    rc::Rc,
    time::{Duration, Instant},
};

use dope::{
    core::{
        Errno,
        driver::{
            life::recovery,
            route::{KeyTag, Tag},
            settings::{demands, errors},
        },
        io::{Reclaimed, event::batch::Flow},
    },
    manifold::timing,
    net::wire,
    runtime::{
        executor::{session, session::Completion},
        topology::{self, error, lending::Stage},
    },
};
use dope_fiber::task::storage::{fixed::schedule::queue, scope::Brand};
use dope_quic::{
    conn::life::tls::clients::Authority,
    proto::cid,
    runtime::{
        endpoint::{
            self, client,
            config::{self, Config},
            server::Plan,
        },
        mux::app::Silent,
    },
};
use dope_test::{
    fibers,
    harness::{GUARD, observe::VecCellExt as _},
    reclaim,
};
use sark_core::life;
use sark_h3::{
    conn::options,
    manifold::server::{self, deadline, failure, rotation, startup},
};
use sark_http::payload::compress::dynamic::hash;

use crate::common::server::h3::{
    Client,
    apps::SrvApp,
    observer::{ConfigError, Probe, StoragePlan, config_with_streams},
    proof::{
        Cancellation, Connect, DIALER, Dispatch, Failure, LISTENER, Metric, Proof, Shared,
        ledger::Verification, request::Spec,
    },
    runtime::{AsyncRuntime, RunPlan},
};

type Result<'a, T> = std::result::Result<T, Error<'a>>;

#[derive(Debug)]
pub enum Error<'a> {
    Overflow(Metric, usize, usize),
    Index(num::TryFromIntError),
    Reserve(io::Error),
    Config(ConfigError),
    Tls(dope_tls::Error),
    Endpoint(endpoint::PlanError),
    Application(server::ConfigError),
    Client(config::Error),
    Capacity(demands::Overflow),
    Claims(errors::Claim),
    Bind(startup::BindError<queue::PrepareError>),
    Dialer(endpoint::BindError),
    Session(session::Error<'a, std::result::Result<(), fibers::Run>>),
    Run(fibers::Run),
    Enter {
        cause: error::Cause<'a, Infallible>,
        output: Option<&'a Result<'a, ()>>,
    },
    Reclaim(&'a reclaim::Error),
    Reclaimed(&'a [Reclaimed]),
    Proof(Failure),
    Fixture(Vec<Failure>),
    Verification(Verification),
    Response {
        slot: usize,
        actual: &'a [u8],
        expected: &'a [u8],
    },
}

pub struct Scenario<'a> {
    pub name: &'a str,
    pub requests: Vec<Spec<'a>>,
    pub expected_bodies: Vec<Option<&'a [u8]>>,
    pub task_capacity: usize,
    pub cancellation: Option<Cancellation>,
    pub must_finish_before_last: Option<usize>,
    pub linked: usize,
}

impl<'a> Scenario<'a> {
    pub(crate) fn try_prepare(
        &mut self,
        protocol: options::Config,
        send_bytes: Option<num::NonZero<usize>>,
    ) -> Result<'_, (topology::Prepared<StoragePlan, RunPlan<'a>>, Rc<Shared<'a>>)> {
        use dope_quic::config;

        if self.requests.is_empty() {
            return Err(Error::Verification(Verification::NoRequests));
        }
        if self.requests.len() != self.expected_bodies.len() {
            return Err(Error::Verification(Verification::Count {
                property: Metric::Oracles,
                actual: self.requests.len(),
                expected: self.expected_bodies.len(),
            }));
        }
        let limit = self
            .requests
            .iter()
            .filter(|request| matches!(request.dispatch, Dispatch::Established | Dispatch::Initial))
            .find_map(|request| {
                (request.expect.error == Some(sark_h3::frame::ErrorCode::ExcessiveLoad))
                    .then_some(request.data.bytes.len())
            });
        let slots = self
            .requests
            .len()
            .checked_mul(8)
            .ok_or(Error::Overflow(
                Metric::RequestSlots,
                self.requests.len(),
                8,
            ))?
            .max(64);
        let endpoint = Config {
            max_conns: 1,
            cid_rotation: cid::Rotation::disabled(),
            opening_lifetime: None,
            outgoing_capacity: slots,
            outgoing_bytes_capacity: slots
                .checked_mul(4096)
                .ok_or(Error::Overflow(Metric::OutgoingBytes, slots, 4096))?
                .max(1 << 20),
            packet_buffer_slots: u32::try_from(slots).map_err(Error::Index)?,
            packet_buffer_bytes: 2048,
        };
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let port = UdpSocket::bind(bind).map_err(Error::Reserve)?;
        let address = port.local_addr().map_err(Error::Reserve)?;
        let identity = config::raw::Identity::from_seed(&[0x71; 32]).map_err(Error::Tls)?;
        let mut client =
            config::raw::Identity::client(identity.public_key()).map_err(Error::Tls)?;
        client.alpn_protocols = vec![b"h3".to_vec()];
        let policy = Authority::try_new(client, address.ip().into())
            .map_err(|error| Error::Tls(error.into()))?;
        let streams = u64::try_from(self.requests.len()).map_err(Error::Index)?;
        let options = config_with_streams(streams, send_bytes).map_err(Error::Config)?;
        let params = options.params().clone();
        let mut proof = Proof::new(
            mem::take(&mut self.requests),
            Instant::now() + Duration::from_secs(10),
            self.cancellation,
        );
        proof.network.connect.push(Connect {
            slot: 0,
            peer: address,
            authority: policy,
            params,
            initial_dcid: vec![0xb7; 8],
        });
        let drops = Rc::clone(&proof.cancel.dropped);
        let errors = Rc::clone(&proof.network.failures);
        let stream = Rc::clone(&proof.stream);
        let proof = Rc::new(Shared::new(proof));
        let mut config = server::Config {
            protocol,
            ..server::Config::default()
        };
        if let Some(limit) = limit {
            config.max_connection_body_bytes = limit;
        }
        let mut tls = identity.server().map_err(Error::Tls)?;
        tls.alpn_protocols = vec![b"h3".to_vec()];
        let plan =
            Plan::<LISTENER>::new(address, tls, options, endpoint).map_err(Error::Endpoint)?;
        let plan = startup::Plan::try_from(plan, config).map_err(Error::Application)?;
        let client = client::Plan::new(bind, endpoint).map_err(Error::Client)?;
        let shared = Rc::clone(&proof);
        let topology = RunPlan {
            plan,
            proof: shared,
            client,
            reservation: port,
            task_capacity: self.task_capacity.try_into().map_err(Error::Capacity)?,
        };
        let prepared = topology::Plan::new(
            StoragePlan {
                cancel_dropped: drops,
                errors,
                stream,
            },
            topology,
        )
        .for_install::<timing::Balanced>()
        .map_err(Error::Claims)?;
        Ok((prepared, proof))
    }

    pub fn try_run_with<L, D, O, Out>(
        mut self,
        protocol: options::Config,
        send_bytes: Option<num::NonZero<usize>>,
        lifetime: L,
        policy: impl for<'d> deadline::Policy<'d>,
        recover: impl recovery::Recover,
        diagnose: D,
        observe: impl FnOnce(Probe<'a, D>) -> O,
        done: impl for<'error> FnOnce(Result<'error, Proof<'a>>) -> Out,
    ) -> Out
    where
        L: for<'d> rotation::Policy<'d>,
        D: Fn(&dyn fmt::Debug),
        O: for<'d> server::Mode<'d, LISTENER> + failure::Packet,
    {
        let observed = <O as life::Mode<
            dope_quic::conn::id::Handle<dope_quic::conn::id::Driver<'static>, LISTENER>,
            dope_quic::wire::stream::Id,
        >>::ENABLED;
        let (prepared, proof) = match self.try_prepare(protocol, send_bytes) {
            Ok(prepared) => prepared,
            Err(error) => return done(Err(error)),
        };
        let routes = [
            <KeyTag<LISTENER> as Tag>::ROUTE,
            <KeyTag<DIALER> as Tag>::ROUTE,
        ];
        let reports = Cell::new(Vec::new());
        let mut reclaim = reclaim::Reports::<_>::new(prepared.layout(), routes);
        let mut finish = Completion::new();
        prepared.enter_with(
            (),
            |scope| {
                scope.lend(|topology, stage| {
                    Brand::with(|brand| {
                        let RunPlan {
                            plan,
                            proof,
                            client,
                            reservation: port,
                            task_capacity: capacity,
                        } = topology;
                        drop(port);
                        let router = SrvApp::multiplexed::<wire::Identity, KeyTag<LISTENER>, _>(
                            brand,
                            stage.storage(),
                            sark_fiber::Config::new(capacity.count().get()),
                        );
                        let runtime = (|| {
                            let seed =
                                hash::Seed::new(stage.driver().driver_ref().random_seed());
                            let deadline = stage
                                .driver()
                                .driver_ref()
                                .clock()
                                .deadline_at(proof.read().map_err(Error::Proof)?.network.deadline);
                            let server = plan
                                .try_with::<Silent, _, _, _, _, _>(
                                    router,
                                    observe(Probe(Rc::clone(&proof), diagnose)),
                                    lifetime,
                                    policy,
                                    server::Tasks::new(),
                                    &mut stage.driver(),
                                )
                                .map_err(Error::Bind)?;
                            let client = client
                                .bind(
                                    Client::new(seed, Rc::clone(&proof)),
                                    &mut stage.driver(),
                                )
                                .map_err(Error::Dialer)?;
                            Ok(AsyncRuntime {
                                server,
                                client,
                                deadline,
                            })
                        })()?;
                        Stage::with_quiesce(stage, &mut finish, runtime, GUARD, |mut app| {
                            proof.failures.try_wait(&mut app)
                        })
                        .map_err(Error::Session)?
                        .map_err(Error::Run)
                    })
                })
            },
            recovery::Policy::new(|report| reports.push(report), recover),
            |outcome| {
                match outcome {
                    Ok(Ok(())) => {},
                    Ok(Err(error)) => return done(Err(error)),
                    Err(error) => return done(Err(Error::Enter {
                        cause: error.cause(), output: error.output(),
                    })),
                }
                for report in reports.take() {
                    reclaim.push(report);
                }
                let events = match reclaim.events() {
                    Ok(events) => events,
                    Err(error) => return done(Err(Error::Reclaim(error))),
                };
                if events.iter().filter(|event| !matches!(event,
                    Reclaimed::RecvRoute { target, result: Ok(_), flow: Flow::More }
                    if routes.contains(&target.route())
                )).count() != routes.len() || routes.iter().any(|route| {
                    events.iter().filter(|event| matches!(event,
                        Reclaimed::RecvRoute { target, result: Err(Errno::CANCELED), flow: Flow::Last }
                        if target.route() == *route)).count() != 1
                }) {
                    return done(Err(Error::Reclaimed(events)));
                }
                let Some(proof) = Rc::into_inner(proof) else {
                    return done(Err(Error::Verification(Verification::RetainedScenario)));
                };
                let proof = proof.into_inner();
                match self.verify(&proof, observed) {
                    Ok(()) => done(Ok(proof)),
                    Err(error) => done(Err(error)),
                }
            },
        )
    }

    fn verify<'e>(&'e self, proof: &'e Proof<'a>, observed: bool) -> Result<'e, ()> {
        let errors = proof.network.failures.values.take();
        if !errors.is_empty() {
            return Err(Error::Fixture(errors));
        }
        if proof.network.rejected != 0 {
            return Err(Error::Verification(Verification::Count {
                property: Metric::RejectedPeers,
                actual: proof.network.rejected,
                expected: 0,
            }));
        }
        for (slot, expected) in self.expected_bodies.iter().enumerate() {
            match expected {
                Some(body) => {
                    if !proof.replies.status_ok[slot]
                        || !proof.replies.finished[slot]
                        || &proof.replies.bodies[slot] != body
                    {
                        return Err(Error::Response {
                            slot,
                            actual: &proof.replies.bodies[slot],
                            expected: body,
                        });
                    }
                }
                None if proof.requests[slot].expect.error.is_some()
                    || proof.requests[slot].expect.connection_closed =>
                {
                    if !proof.replies.error_ok[slot] || !proof.replies.finished[slot] {
                        return Err(Error::Verification(Verification::ResponseError));
                    }
                }
                None if self.cancellation.is_none() => {
                    return Err(Error::Verification(Verification::MissingOracle));
                }
                None => {}
            }
        }
        if let Some(slot) = self.must_finish_before_last {
            let order = &proof.replies.completion_order;
            if !order
                .iter()
                .position(|item| *item == slot)
                .is_some_and(|position| position + 1 < order.len())
            {
                return Err(Error::Verification(Verification::Fairness));
            }
        }
        if !observed {
            return if proof.trace.events.is_empty() && proof.trace.audits.is_empty() {
                Ok(())
            } else {
                Err(Error::Proof(Failure::Scenario(
                    "quiet lifecycle events".to_owned(),
                )))
            };
        }
        proof
            .trace
            .verify(
                &proof.network.endpoint,
                self.linked,
                proof
                    .requests
                    .iter()
                    .zip(&proof.replies.streams)
                    .map(|(request, stream)| (*stream, request.expect.input_finished)),
            )
            .map_err(Error::Verification)?;
        Ok(())
    }
}
