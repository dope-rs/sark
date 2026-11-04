use std::{
    cell::Cell, convert::Infallible, net::SocketAddr, num::ParseFloatError, ops::ControlFlow,
    str::Utf8Error, time::Duration,
};

use cartel_redis::{
    client::{plan, settings},
    value::{list::pair, reply, source},
};
use dope::{
    core::{
        BranchError,
        driver::{
            route::{KeyTag, table::conn},
            storage::With,
        },
        platform::affinity::Cpu,
    },
    manifold::{BuildError, app::env::Bundle, service},
    net::{
        tcp::Tcp,
        wire::{Identity, domain},
    },
    runtime::{executor::session, process, shutdown, topology::ops},
};
use dope_fiber::task::storage::scope::Brand;
use sark_fiber::{Throughput, server::PrepareError};
use sark_h1::server::{date::Clock, listener, run};
use sark_http::payload::response::status;
use sark_json::{
    o3::buffer::write::Sink,
    writing::{self, Size, TryEncode, Writer},
};
use settings::init::Plan;

use crate::{
    DATE_UPDATER_ID, HTTP_LISTENER_ID, MAX_CONNECTIONS, REDIS_CONNECTOR_ID,
    error::{self, Error},
    input, worker,
};

pub(super) fn start<'signal>(
    signal: o3::mem::slot::Vacant<'signal, dope::core::platform::signal::Receiver>,
    label: &str,
) -> Result<(), Error<'signal, settings::Error, Setup>> {
    let input::Redis {
        bind: input::Bind { bind },
        redis_addr: upstream,
    } = input::Redis::try_parse().map_err(Error::Input)?;
    let server = listener::Config::<KeyTag<HTTP_LISTENER_ID>, Throughput>::tcp(
        bind,
        conn::Capacity::fixed::<MAX_CONNECTIONS>(),
        Duration::from_secs(10),
    );

    error::write(format_args!(
        "sark {label}: listening on http://{bind}, upstream redis {upstream}"
    ))
    .map_err(Error::Output)?;
    let config = config()?;
    let cpus = Cpu::current().map_err(Error::Affinity)?;
    let layout = process::Layout::pinned(cpus.into_iter().take(1)).map_err(Error::Create)?;
    layout
        .try_assign([Server {
            server,
            upstream,
            config,
        }])
        .map_err(Error::Create)?
        .run(signal, Server::run)
        .map_err(Error::Run)
}

pub(super) type Setup = BranchError<plan::Error, BranchError<PrepareError, worker::Error>>;

struct Server {
    server: listener::Config<KeyTag<HTTP_LISTENER_ID>, Throughput>,
    upstream: SocketAddr,
    config: settings::Config,
}

impl Server {
    fn run<'run>(
        self,
        context: process::Context<'run>,
    ) -> Result<shutdown::Requested<process::Shutdown<'run>>, Setup> {
        let Self {
            server,
            upstream,
            config,
        } = self;
        let redis = plan::Plan::<REDIS_CONNECTOR_ID>::new(config);
        let snapshot = service::Snapshot::from_one(
            service::Revision::new(1),
            service::Endpoint::new(upstream, upstream),
        );
        let assembly = redis
            .assemble::<Bundle<Tcp, Identity, Throughput>>(
                snapshot,
                settings::DEFAULT_BACKOFF,
                Duration::from_secs(30),
                Plan::new(),
            )
            .map_err(BranchError::First)?;
        let output = Cell::new(None);
        let (plan, config, recovery) = (
            server.plan::<KeyTag<DATE_UPDATER_ID>>(),
            sark_fiber::Config::new(MAX_CONNECTIONS),
            worker::observe(&output),
        );
        let clock = Clock::new()
            .map_err(PrepareError::Date)
            .map_err(BranchError::First)
            .map_err(BranchError::Second)?;
        let (storage, provider) = assembly.into_parts();
        let storage = With::new(storage, clock);
        let mut completion = session::Completion::new();
        let report = &mut completion;
        let profile = plan.config();
        let plan = provider.compose(storage, plan, move |plan, owner, stage| {
            stage.hold(owner, |held, stage| {
                Brand::with(|brand| {
                    plan.prepare_in(
                        stage,
                        domain::Local::new(Identity),
                        |prepared, wait, stage| {
                            let result = held.lend_scope(stage, report, |issued| {
                                let (client, permit) = issued.into_parts();
                                let state = std::pin::pin!(client);
                                crate::App::run_prepared(
                                    brand,
                                    state.as_ref(),
                                    config,
                                    prepared,
                                    wait,
                                    permit,
                                )
                            });
                            run::Failure::lending(result)
                        },
                    )
                    .map_err(|error| run::Failure::Install(BuildError::Create(error)))?
                })
            })
        });
        let prepared = plan
            .with_config(profile)
            .map_err(PrepareError::Layout)
            .map_err(BranchError::First)
            .map_err(BranchError::Second)?;
        prepared
            .enter(
                context,
                ops::Map(ops::Compose, |result| match result {
                    Ok(Ok(request)) => Ok(request),
                    Ok(Err(error)) => Err(run::Failure::map_install(error, BranchError::Second)),
                    Err(error) => Err(run::Failure::Install(BranchError::First(error))),
                }),
                recovery,
                |outcome| {
                    let result = match outcome {
                        Ok(result) => result.map_err(run::RunError::Failure),
                        Err(error) => Err(run::RunError::from(error)),
                    };
                    worker::finish_run(result, &output)
                },
            )
            .map_err(BranchError::Second)
            .map_err(BranchError::Second)
    }
}

pub enum Failure<E, R = Infallible, B = Infallible> {
    Empty,
    Utf8 { value: B, error: Utf8Error },
    Number { value: B, error: ParseFloatError },
    Nonfinite { value: B, number: f64 },
    Member(pair::TextError<R>),
    InvalidCount(usize),
    EmptyUser,
    Missing,
    Reply(E),
}

impl<E: source::Error, R, B> Failure<E, R, B> {
    pub fn status(&self) -> status::Code {
        match self {
            Self::Empty => status::Code::OK,
            Self::Utf8 { .. }
            | Self::Number { .. }
            | Self::Nonfinite { .. }
            | Self::InvalidCount(_)
            | Self::EmptyUser => status::Code::BAD_REQUEST,
            Self::Missing => status::Code::NOT_FOUND,
            Self::Member(_) | Self::Reply(_) => status::Code::BAD_GATEWAY,
        }
    }

    /// Fixed ASCII wire text without JSON escapes.
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Empty => b"",
            Self::Utf8 { .. } | Self::Number { .. } | Self::Nonfinite { .. } => {
                b"user must be non-empty and value must be finite"
            }
            Self::Member(_) => b"redis member must be valid UTF-8",
            Self::InvalidCount(_) => b"n must be between 1 and 100",
            Self::EmptyUser => b"user must be non-empty",
            Self::Missing => b"not found",
            Self::Reply(error) => match error.kind() {
                reply::Kind::Control => b"redis control failure",
                reply::Kind::Server => b"redis server failure",
                reply::Kind::Number => b"redis number failure",
                reply::Kind::Decode => b"redis decode failure",
            },
        }
    }
}

impl<E: source::Error, R, B> Size for Failure<E, R, B> {
    fn json_size(&self) -> usize {
        self.bytes().len() + 2
    }
}

impl<E: source::Error, R, B> TryEncode for Failure<E, R, B> {
    fn try_write<'a, W: Sink>(
        &self,
        writer: Writer<'a, W>,
    ) -> ControlFlow<writing::Failure<'a, W>, Writer<'a, W>> {
        writer.put(b"\"")?.put(self.bytes())?.put(b"\"")
    }
}

fn config() -> Result<settings::Config, settings::Error> {
    settings::Config::new(settings::Capacities {
        connections: 4,
        admission_waiters: 2048,
        inflight: 2048,
        request_frame_bytes: 4 * 1024,
        response_bytes: 64 * 1024 * 1024,
        response_values: 65_536,
        response_frame_bytes: 16 * 1024 * 1024,
        response_frame_values: 16_384,
    })
}
