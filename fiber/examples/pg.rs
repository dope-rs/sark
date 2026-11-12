use std::{
    net::SocketAddr,
    num::{self, NonZero},
    process::{ExitCode, Termination},
    time::Duration,
};

use dope::{
    core::{driver::route::KeyTag, platform::affinity::Cpu},
    manifold::service::health,
};
use o3::buffer::storage::Shared;

mod body;
mod error;
use error::Error;
pub mod input;
mod worker;

use cartel_pg::{
    Config, PgRow, PgTable,
    api::{
        results::{self, Collection, Validated},
        traits::{Begin as _, Client, Status as _, Transaction as _},
    },
    pg_instance,
    port::{Port, config},
    protocol::PickPolicy,
    query_group,
};
use dope::{
    core::{
        BranchError,
        driver::{route::table::conn, storage::With},
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
use sark_json::writing;

type Row<P> = Validated<results::Row<P, UserById>>;
type Rows<B> = Validated<Collection<B, UserPage>>;
type Response<T> = writing::Response<body::Body<T>>;

const MAX_CONNECTIONS: usize = 1024;
const HTTP_LISTENER_ID: u8 = 0;
const DATE_UPDATER_ID: u8 = 1;
const PG_CONNECTOR_ID: u8 = 2;
const PG_CONNECTIONS: usize = 4;
const PG_PENDING_REQUESTS_PER_CONNECTION: usize = 16;
const PG_REQUEST_FRAMES: usize = PG_CONNECTIONS * PG_PENDING_REQUESTS_PER_CONNECTION;
const PG_REQUEST_FRAME_CAPACITY: usize = 4 * 1024;
const PG_RESPONSE_BUFFER_CAPACITY: usize = 256 * 1024 * 1024;
const PG_RESPONSE_FRAME_CAPACITY: usize = 16 * 1024 * 1024;
const PG_RESPONSE_ITEMS: usize = 65_536;
const PG_INFLIGHT: usize = PG_REQUEST_FRAMES;
const PG_WAITERS: usize = PG_REQUEST_FRAMES;
const PG_NOTIFICATIONS: usize = 1024;
const PG_NOTIFICATION_BUFFER_CAPACITY: usize = 4 * 1024 * 1024;
const USER_PAGE_CAPACITY: i64 = 100;
const PG_RECONNECT_BACKOFF: Duration = Duration::from_millis(500);

#[derive(PgTable, PgRow)]
struct User {
    id: i64,
    name: String,
}

#[query_group]
impl User {
    fn by_id(id: i64) -> User {
        User::filter(User::id == id).one()
    }

    fn page(after_id: i64, limit: i64) -> Vec<User> {
        User::filter(User::id > after_id)
            .order_by(User::id)
            .limit(limit)
            .all()
    }

    fn rename(id: i64, new_name: &str) {
        User::filter(User::id == id).update(User::name = new_name)
    }
}

pg_instance! { Db: User }

type Setup = BranchError<
    BranchError<cartel_pg::ConfigError, health::Error>,
    BranchError<PrepareError, worker::Error>,
>;

#[sark_json::json(ordered)]
struct Rename {
    name: Shared,
}

#[sark::request]
struct Lookup {
    #[path]
    id: usize,
}

#[sark::request]
#[json_body(Rename)]
struct Tx {
    #[path]
    id: usize,
}

#[sark::handler]
async fn get_user<C>(request: Lookup, pg: &C) -> Result<Response<Row<C::Payload>>, body::Error<'_>>
where
    C: Client<'_, PG_CONNECTOR_ID, Schema = Db>,
{
    let id = user_id(request.id)?;
    let user = User::by_id(*pg, id.get()).await?;
    Ok(body::response(user.validate()?)?)
}

#[sark::handler]
async fn rename_in_tx<C>(request: Tx, pg: &C) -> Result<Response<()>, body::Error<'_>>
where
    C: Client<'_, PG_CONNECTOR_ID, Schema = Db>,
{
    let name = request.body.name.as_str();
    if name.is_empty() || name.len() > 128 {
        return Err(body::Error::invalid_name());
    }
    let id = user_id(request.id)?;
    let tx = pg.transaction().begin().await?;
    let outcome = User::rename(&tx, id.get(), name).await;
    tx.finish(outcome).await?;
    Ok(body::response(())?)
}

#[sark::handler]
async fn list_users<C>(pg: &C) -> Result<Response<Rows<C::Batch>>, body::Error<'_>>
where
    C: Client<'_, PG_CONNECTOR_ID, Schema = Db>,
{
    let users = User::page(*pg, 0, USER_PAGE_CAPACITY).await?;
    Ok(body::response(users.validate()?)?)
}

fn user_id(id: usize) -> Result<NonZero<i64>, num::TryFromIntError> {
    NonZero::try_from(i64::try_from(id)?)
}

sark::define_route! {
    App<C>: C where C: Client<'_, PG_CONNECTOR_ID, Schema = Db> => {
        GET "/users/:id" => request(path) async(capacity = MAX_CONNECTIONS, body = discarded) get_user,
        POST "/tx/:id" => request(path) async(capacity = MAX_CONNECTIONS, body = json) rename_in_tx,
        GET "/users" => async(capacity = MAX_CONNECTIONS, body = discarded) list_users,
    }
}

struct Server {
    user: String,
    secret: String,
    database: String,
    addr: SocketAddr,
    limits: config::Config,
    server: listener::Config<KeyTag<HTTP_LISTENER_ID>, Throughput>,
    policy: PickPolicy,
}

impl Server {
    fn run<'run>(
        self,
        context: process::Context<'run>,
    ) -> Result<shutdown::Requested<process::Shutdown<'run>>, Setup> {
        let Self {
            user,
            secret,
            database,
            addr,
            limits,
            server,
            policy,
        } = self;
        let auth = Config::new(user, secret, database)
            .map_err(|error| BranchError::First(BranchError::First(error)))?;
        let port = Port::<Db, PG_CONNECTOR_ID>::plan(auth, limits);
        let snapshot = service::Snapshot::from_one(
            service::Revision::new(1),
            service::Endpoint::new(addr, addr),
        );
        let assembly = port
            .assemble::<Bundle<Tcp, Identity, Throughput>>(
                snapshot,
                PG_RECONNECT_BACKOFF,
                Duration::from_secs(30),
            )
            .map_err(|error| BranchError::First(BranchError::Second(error)))?;
        let output = std::cell::Cell::new(None);
        let (plan, config, recovery) = (
            server.plan::<KeyTag<DATE_UPDATER_ID>>(),
            sark_fiber::Config::new(MAX_CONNECTIONS),
            worker::observe(&output),
        );
        let clock = Clock::new()
            .map_err(|error| BranchError::Second(BranchError::First(PrepareError::Date(error))))?;
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
                                let state = std::pin::pin!({
                                    client.status().set_policy(policy);
                                    client
                                });
                                App::run_prepared(
                                    brand,
                                    state.as_ref(),
                                    config,
                                    prepared,
                                    wait,
                                    permit,
                                )
                            });
                            run::lending(result)
                        },
                    )
                    .map_err(|error| run::Failure::Install(BuildError::Create(error)))?
                })
            })
        });
        let prepared = plan.with_config(profile).map_err(|error| {
            BranchError::Second(BranchError::First(PrepareError::Layout(error)))
        })?;
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
            .map_err(|error| BranchError::Second(BranchError::Second(error)))
    }
}

fn start<'signal>(
    signal: o3::mem::slot::Vacant<'signal, dope::core::platform::signal::Receiver>,
) -> Result<(), Error<'signal, config::Error, Setup>> {
    let input::Postgres {
        bind: input::Bind { bind },
        pg_addr,
        pg_user,
        pg_password: secret,
        pg_database: database,
        pg_pick_policy: policy,
    } = input::Postgres::try_parse().map_err(Error::Input)?;
    let limits = config::Config::new(config::Capacities {
        connections: PG_CONNECTIONS,
        inflight: PG_INFLIGHT,
        admission_waiters: PG_WAITERS,
        requests: config::Request {
            frames: PG_REQUEST_FRAMES,
            frame_bytes: PG_REQUEST_FRAME_CAPACITY,
        },
        responses: config::Response {
            items: PG_RESPONSE_ITEMS,
            bytes: PG_RESPONSE_BUFFER_CAPACITY,
            frame_bytes: PG_RESPONSE_FRAME_CAPACITY,
        },
        notifications: config::Notification {
            items: PG_NOTIFICATIONS,
            bytes: PG_NOTIFICATION_BUFFER_CAPACITY,
        },
    })?;
    let server = listener::Config::<KeyTag<HTTP_LISTENER_ID>, Throughput>::tcp(
        bind,
        conn::Capacity::fixed::<MAX_CONNECTIONS>(),
        Duration::from_secs(10),
    );

    error::write(format_args!(
        "sark pg: listening on http://{bind}, upstream pg {pg_addr}, pick={policy:?}"
    ))
    .map_err(Error::Output)?;

    let cpus = Cpu::current().map_err(Error::Affinity)?;
    let layout = process::Layout::pinned(cpus.into_iter().take(1)).map_err(Error::Create)?;
    layout
        .try_assign([Server {
            user: pg_user,
            secret,
            database,
            addr: pg_addr,
            limits,
            server,
            policy,
        }])
        .map_err(Error::Create)?
        .run(signal, Server::run)
        .map_err(Error::Run)?;
    Ok(())
}

fn main() -> ExitCode {
    let mut signal = std::mem::MaybeUninit::uninit();
    match start(o3::mem::slot::Vacant::new(&mut signal)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => error.report(),
    }
}
