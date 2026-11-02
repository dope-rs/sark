//! Provider clients and state retain borrows; unused declarations validate literals.
//!
//! ```
//! # mod common;
//! use std::cell::Cell;
//! use common::route::glue::task::{self, generics::{self, Source, State}};
//! use dope::{
//!     core::{BranchError, driver::{route::KeyTag, storage::With}},
//!     manifold::{BuildError, app::env::Bundle},
//!     net::{tcp::Tcp, wire::{Identity, domain}},
//!     runtime::{executor::session, process, topology::{self, lending, ops}},
//! };
//! use dope::core::driver::life::recovery::{Policy, Recovery};
//! use std::convert::Infallible;
//! use dope_fiber::task::storage::scope::Brand;
//! use sark_fiber::{Config, Throughput, server::PrepareError};
//! use sark_h1::server::date::Clock;
//!
//! sark::define_route! {
//!     pub r#App<__W, const r#producer: usize>: State<__W, r#producer>
//!     where __W: Source<r#producer>
//!     => {
//!         GET "/" => async(capacity = 2, body = discarded) generics::reply,
//!     }
//! }
//!
//! fn serve<const storage: usize>(
//!     context: process::Context<'_>,
//!     assembly: topology::Plan<(), cartel_redis::client::Node<2, Bundle<Tcp, Identity, Throughput>>>,
//!     plan: sark_h1::server::run::Plan<KeyTag<0>, KeyTag<1>, Throughput>,
//!     first: &[u8],
//!     encoded: &Cell<usize>,
//! ) -> Result<(), PrepareError> {
//!     let mut calls = 0;
//!     let (config, recovery, mut finish) = (
//!         Config::new(2),
//!         Policy::new(drop, |error: Recovery<'_, '_>| -> Infallible {
//!             panic!("native recovery: {:?}", error.cause())
//!         }),
//!         |result: Result<_, topology::RunError<'_, _, _, _, _, _>>| {
//!             calls += 1;
//!             result.as_ref().map(|_| ()).map_err(|error| error.cause()).expect("server");
//!         },
//!     );
//!     let (owner, provider) = assembly.into_parts();
//!     let owner = With::new(owner, Clock::new().map_err(PrepareError::Date)?);
//!     let mut completion = session::Completion::new();
//!     let report = &mut completion;
//!     let profile = plan.config();
//!     let plan = provider.compose(owner, plan, move |plan, owner, stage| {
//!         stage.hold(owner, |held, stage| {
//!             Brand::with(|brand| {
//!                 let result = plan.prepare_in(stage, domain::Local::new(Identity), |prepared, stage| {
//!                     held.lend_scope(stage, report, |issued| {
//!                         let (client, permit) = issued.into_parts();
//!                         let state = std::pin::pin!(State::<_, 2> {
//!                             source: (client, task::State { first, second: b"", encoded }),
//!                         });
//!                         App::run_prepared(
//!                             brand, state.as_ref(), config, prepared, permit,
//!                         )
//!                     })
//!                 }).map_err(|error| topology::Failure::Install(BuildError::Create(error)))?;
//!                 match result {
//!                     Ok(Ok(request)) => Ok(request),
//!                     Ok(Err(error)) => Err(topology::Failure::Run(session::Error::Drive(error))),
//!                     Err(lending::Error::Build(error)) => Err(topology::Failure::Install(error)),
//!                     Err(lending::Error::Run(error)) => Err(topology::Failure::Run(error)),
//!                 }
//!             })
//!         })
//!     });
//!     let prepared = plan.with_config(profile).map_err(PrepareError::Layout)?;
//!     let _ = prepared.enter(
//!         context,
//!         ops::Map(ops::Compose, |result| match result {
//!             Ok(Ok(request)) => Ok(request),
//!             Ok(Err(topology::Failure::Run(error))) => Err(topology::Failure::Run(error)),
//!             Ok(Err(topology::Failure::Install(error))) =>
//!                 Err(topology::Failure::Install(BranchError::Second(error))),
//!             Err(error) => Err(topology::Failure::Install(BranchError::First(error))),
//!         }),
//!         recovery,
//!         |outcome| finish(match outcome {
//!             Ok(result) => result.map_err(topology::RunError::Failure),
//!             Err(error) => Err(topology::RunError::from(error)),
//!         }),
//!     );
//!     Ok(())
//! }
//! # fn main() {}
//! ```
//!
//!
//! ```compile_fail,E0521
//! # mod common;
//! use std::cell::Cell;
//! use common::route::glue::task::{self, generics::{App, State}};
//! use dope::{
//!     core::{BranchError, driver::{route::KeyTag, storage::With}},
//!     manifold::{BuildError, app::env::Bundle},
//!     net::{tcp::Tcp, wire::{Identity, domain}},
//!     runtime::{executor::session, process, topology::{self, lending, ops}},
//! };
//! use dope::core::driver::life::recovery::{Policy, Recovery};
//! use std::convert::Infallible;
//! use dope_fiber::task::storage::scope::Brand;
//! use sark_fiber::{Config, Throughput, server::PrepareError};
//! use sark_h1::server::date::Clock;
//!
//! fn escape(
//!     context: process::Context<'static>,
//!     assembly: topology::Plan<(), cartel_redis::client::Node<2, Bundle<Tcp, Identity, Throughput>>>,
//!     plan: sark_h1::server::run::Plan<KeyTag<0>, KeyTag<1>, Throughput>,
//!     first: &[u8],
//!     encoded: &Cell<usize>,
//! ) -> Result<(), PrepareError> {
//!     let mut escaped = None;
//!     let slot = &mut escaped;
//!     let (config, recovery, finish) = (
//!         Config::new(2),
//!         Policy::new(drop, |error: Recovery<'_, '_>| -> Infallible {
//!             panic!("native recovery: {:?}", error.cause())
//!         }),
//!         |result: Result<_, topology::RunError<'_, _, _, _, _, _>>| {
//!             result.as_ref().map(|_| ()).map_err(|error| error.cause()).expect("server");
//!         },
//!     );
//!     let (owner, provider) = assembly.into_parts();
//!     let owner = With::new(owner, Clock::new().map_err(PrepareError::Date)?);
//!     let mut completion = session::Completion::new();
//!     let report = &mut completion;
//!     let profile = plan.config();
//!     let plan = provider.compose(owner, plan, move |plan, owner, stage| {
//!         stage.hold(owner, |held, stage| {
//!             Brand::with(|brand| {
//!                 let result = plan.prepare_in(stage, domain::Local::new(Identity), |prepared, stage| {
//!                     held.lend_scope(stage, report, |issued| {
//!                         let (client, permit) = issued.into_parts();
//!                         *slot = Some(client);
//!                         let state = std::pin::pin!(State::<_, 2> {
//!                             source: (client, task::State { first, second: b"", encoded }),
//!                         });
//!                         App::run_prepared(
//!                             brand, state.as_ref(), config, prepared, permit,
//!                         )
//!                     })
//!                 }).map_err(|error| topology::Failure::Install(BuildError::Create(error)))?;
//!                 match result {
//!                     Ok(Ok(request)) => Ok(request),
//!                     Ok(Err(error)) => Err(topology::Failure::Run(session::Error::Drive(error))),
//!                     Err(lending::Error::Build(error)) => Err(topology::Failure::Install(error)),
//!                     Err(lending::Error::Run(error)) => Err(topology::Failure::Run(error)),
//!                 }
//!             })
//!         })
//!     });
//!     let prepared = plan.with_config(profile).map_err(PrepareError::Layout)?;
//!     let _ = prepared.enter(
//!         context,
//!         ops::Map(ops::Compose, |result| match result {
//!             Ok(Ok(request)) => Ok(request),
//!             Ok(Err(topology::Failure::Run(error))) => Err(topology::Failure::Run(error)),
//!             Ok(Err(topology::Failure::Install(error))) =>
//!                 Err(topology::Failure::Install(BranchError::Second(error))),
//!             Err(error) => Err(topology::Failure::Install(BranchError::First(error))),
//!         }),
//!         recovery,
//!         |outcome| finish(match outcome {
//!             Ok(result) => result.map_err(topology::RunError::Failure),
//!             Err(error) => Err(topology::RunError::from(error)),
//!         }),
//!     );
//!     drop(escaped);
//!     Ok(())
//! }
//! # fn main() {}
//! ```

//! HTTP request inputs may end before the returned response's storage borrow.
//!
//! ```
//! use sark_client::reply;
//! use sark_fiber::client::Request;
//! use sark_http::method::Method;
//!
//! #[dope_fiber::fiber_fn('d)]
//! async fn local_inputs<'app, 'd: 'app, C: Request<'app, 'd> + ?Sized>(
//!     client: &C,
//! ) -> Result<reply::Scoped<'app, 'd>, C::Error> {
//!     let path = String::from("/effect");
//!     let value = String::from("local");
//!     let headers = [("x-request", value.as_str())];
//!     let body = [1, 2, 3];
//!     client.send_with(Method::POST, &path, &headers, &body).await
//! }
//! # fn main() {}
//! ```
//!
//!
//! ```compile_fail
//! use sark_client::reply;
//! use sark_fiber::client::Request;
//! use sark_http::method::Method;
//!
//! #[dope_fiber::fiber_fn('d)]
//! async fn escape<'app, 'd: 'static, C: Request<'app, 'd> + ?Sized>(
//!     client: &C,
//! ) -> Result<reply::Scoped<'static, 'd>, C::Error> {
//!     client.send_with(Method::GET, "/", &[], &[]).await
//! }
//! # fn main() {}
//! ```

//! Optional operations retain their provider owners independently of the call borrow.
//!
//! ```
//! use sark_client::reply;
//! use sark_fiber::client::{Host, Once, Ready};
//! use sark_http::method::Method;
//!
//! fn authority<'app, C: Host<'app>>(client: C) -> &'app str {
//!     client.host()
//! }
//!
//! #[dope_fiber::fiber_fn('d)]
//! async fn local_once<'app, 'd: 'app, C: Once<'app, 'd> + ?Sized>(
//!     client: &C,
//! ) -> Result<reply::Scoped<'app, 'd>, C::Error> {
//!     let path = String::from("/token");
//!     let value = String::from("local");
//!     let headers = [("x-request", value.as_str())];
//!     let body = [1, 2, 3];
//!     client.send_once(Method::POST, &path, &headers, &body).await
//! }
//!
//! #[dope_fiber::fiber_fn('d)]
//! async fn connected<'d, C: Ready<'d> + ?Sized>(client: &C) -> Result<usize, C::Error> {
//!     client.wait_active().await?;
//!     Ok(client.connections())
//! }
//! # fn main() {}
//! ```
//!
//!
//! ```compile_fail
//! use sark_fiber::client::Host;
//!
//! fn escape<'app, C: Host<'app>>(client: C) -> &'static str {
//!     client.host()
//! }
//! # fn main() {}
//! ```

//! ```
//! use dope::{core::driver::{self, route::KeyTag}, manifold::app::creation, net::wire::{self, terminal}};
//! use sark_client::conn::Cause;
//! use sark_fiber::client::{channel, config::report};
//!
//! fn retained<'w, 'd, W: wire::Spec + 'w, C>(
//!     plan: channel::Plan<KeyTag<0>, W, report::Retain>,
//!     owner: &mut C,
//!     context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
//!     run: impl for<'f, 'e, 'b, 'c> FnOnce(
//!         channel::Port<'f, 'e, 'b, 'c, 'w, 'd, KeyTag<0>, W, report::Retain>,
//!         &mut C,
//!     ) -> Cause<'f, terminal::Error<'w, W>>,
//! ) -> Result<(), creation::Error> {
//!     plan.try_with(owner, context, |port, owner| drop(run(port, owner)))
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! use dope::{core::driver::{self, route::KeyTag}, manifold::app::creation, net::wire::{self, terminal}};
//! use sark_client::conn::Cause;
//! use sark_fiber::client::{channel, config::report};
//!
//! fn escape<'w, 'd, W: wire::Spec + 'w, C>(
//!     plan: channel::Plan<KeyTag<0>, W, report::Retain>,
//!     owner: &mut C,
//!     context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
//!     run: impl for<'f, 'e, 'b, 'c> FnOnce(
//!         channel::Port<'f, 'e, 'b, 'c, 'w, 'd, KeyTag<0>, W, report::Retain>,
//!         &mut C,
//!     ) -> Cause<'f, terminal::Error<'w, W>>,
//! ) -> Result<Cause<'w, terminal::Error<'w, W>>, creation::Error> {
//!     plan.try_with(owner, context, run)
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! use dope::{core::driver::{self, route::KeyTag}, manifold::app::creation};
//! use dope_tls::tls::{roles, Tls};
//! use sark_fiber::client::{channel, config::report};
//!
//! fn no_sink<'d, C>(
//!     plan: channel::Plan<KeyTag<0>, Tls<roles::Client>, report::Never>,
//!     owner: &mut C,
//!     context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
//! ) -> Result<(), creation::Error> {
//!     plan.try_with(owner, context, |_, _| ())
//! }
//! # fn main() {}
//! ```

//! ```
//! # mod common;
//! use core::{assert as matches, concat as stringify, stringify as assert};
//! use dope_gen::Application as Clone;
//! use sark_http::{field::Value, payload::{fixed, response::{headers::Name, status::Code}}};
//!
//! #[sark_fiber::response(raw)]
//! struct Reply<'d> {
//!     status: Code,
//!     body: fixed::Body<'d>,
//! }
//!
//! const _: () = matches!(true);
//! const _: &str = stringify!("local");
//! const _: &str = assert!(local);
//! const __sark_evidence: usize = 0;
//! const __head: () = ();
//! const headers: usize = 0;
//! const sink: usize = 0;
//!
//! sark_fiber::define_router! {
//!     pub Router => {
//!         GET "/" => common::route::glue::agnostic::plain_h,
//!     }
//! }
//!
//! #[sark::request]
//! pub struct Request {
//!     #[header("X-Key")]
//!     pub key: r#Option<r#u64>,
//! }
//! type __named_fn<'a> = Request<'a>;
//!
//! #[sark_fiber::handler]
//! async fn named(__sark_id: __named_fn, __sark_input: &u64) -> Reply<'d> {
//!     Reply {
//!         status: if __sark_id.key == Some(*__sark_input) { Code::OK } else { Code::BAD_REQUEST },
//!         body: fixed::Body::new(b"ok"),
//!     }
//! }
//!
//! #[sark_fiber::handler]
//! fn native_const<const params: usize, const __native_const_fn: usize>(
//!     _: &([u8; params], [u8; __native_const_fn]),
//! ) -> Reply<'d> {
//!     Reply { status: Code::OK, body: fixed::Body::new(b"ok") }
//! }
//!
//! #[sark_fiber::handler]
//! async fn task_const<const __sark_input: usize, const __task_const_fn: usize>(
//!     _: &([u8; __sark_input], [u8; __task_const_fn]),
//! ) -> Reply<'d> {
//!     Reply { status: Code::OK, body: fixed::Body::new(b"ok") }
//! }
//!
//! mod restricted {
//!     #[sark_fiber::handler(pub(in self::super))]
//!     fn scoped() -> super::Reply<'d> {
//!         super::Reply { status: super::Code::OK, body: super::fixed::Body::new(b"ok") }
//!     }
//! }
//!
//! sark_fiber::define_route! {
//!     Private: ([u8; 1], [u8; 2]) => {
//!         GET "/" => native_const,
//!         POST "/" => async(capacity = 1, body = discarded) task_const,
//!         GET "/restricted" => restricted::scoped,
//!     }
//! }
//!
//! #[sark::response(raw)]
//! #[header_static("X-Static", "valid\tvalue")]
//! pub struct Response<'d> {
//!     pub status: Code,
//!     pub body: fixed::Body<'d>,
//!     #[header("X-Dynamic")]
//!     pub value: Value<&'d [u8]>,
//! }
//!
//! pub fn name<const __sark_admitted: usize>(input: core::ops::ControlFlow<()>) -> Name<&'static [u8]> {
//!     use core::ops::ControlFlow::{Break as Ok, Continue as Err};
//!     match input {
//!         Ok(()) => sark_fiber::header_name!("Content-Type"),
//!         Err(()) => sark_fiber::header_name!("X-Key"),
//!     }
//! }
//! # fn main() {
//! #     #[sark_fiber::handler]
//! #     async fn local() -> Reply<'d> { Reply { status: Code::OK, body: fixed::Body::new(b"ok") } }
//! #     let _ = <__SarkHandlerLocalX6C6F63616C as
//! #         sark_fiber::manifold::runtime::TaskRoute<'static, ()>>::invoke_task;
//! # }
//! ```
//!
//! ```compile_fail,E0080
//! pub fn unused() {
//!     let _ = sark_fiber::header_name!("");
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail,E0080
//! use sark_http::{field::Value, payload::{fixed, response::status::Code}};
//! #[sark::response(raw)]
//! pub struct Unused<'d> {
//!     pub status: Code,
//!     pub body: fixed::Body<'d>,
//!     #[header("bad name")]
//!     pub value: Value<&'d [u8]>,
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail,E0080
//! use sark_http::payload::{fixed, response::status::Code};
//! #[sark::response(raw)]
//! #[header_static("Content-Length", "1")]
//! pub struct Unused<'d> {
//!     pub status: Code,
//!     pub body: fixed::Body<'d>,
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail,E0080
//! use sark_http::payload::{fixed, response::status::Code};
//! #[sark::response(raw)]
//! #[header_static("x-key", "bad\r\n")]
//! pub struct Unused<'d> {
//!     pub status: Code,
//!     pub body: fixed::Body<'d>,
//! }
//! # fn main() {}
//! ```
//!
//! ```compile_fail,E0080
//! #[sark::request]
//! pub struct Unused {
//!     #[header("bad name")]
//!     pub value: Option<u64>,
//! }
//! # fn main() {}
//! ```

//! Generated route locals remain hygienic under hostile value names.
//!
//! ```
//! # mod common;
//! const body: usize = 0;
//! const slot: usize = 0;
//! const visit: usize = 0;
//! const each: usize = 0;
//! const map: usize = 0;
//! const fold: usize = 0;
//! const retained: usize = 0;
//! const output: usize = 0;
//! const state: usize = 0;
//! const timer: usize = 0;
//! const lifecycle: usize = 0;
//! const marker: usize = 0;
//! const core: usize = 0;
//! const stamp: usize = 0;
//! const route: usize = 0;
//! const tasks: usize = 0;
//! const task_producers: usize = 0;
//! const ids: usize = 0;
//! const response_cache: usize = 0;
//! const gzip: usize = 0;
//! const head: usize = 0;
//! const method: usize = 0;
//! const content_length: usize = 0;
//! const boundary: usize = 0;
//! const limit: usize = 0;
//! const build: usize = 0;
//! const segment: usize = 0;
//! const body_len: usize = 0;
//! const budget: usize = 0;
//! const observer: usize = 0;
//! const lifetime: usize = 0;
//! const error: usize = 0;
//! const never: usize = 0;
//! const __sink: usize = 0;
//! const __output: usize = 0;
//!
//! sark_fiber::define_router! {
//!     Router => {
//!         GET "/" => common::route::glue::agnostic::plain_h,
//!     }
//! }
//! sark_fiber::define_route! {
//!     SyncApp: sark_fiber::EmptyState => {
//!         GET "/" => common::route::glue::agnostic::plain_h,
//!     }
//! }
//! sark_fiber::define_route! {
//!     App: sark_fiber::EmptyState => {
//!         GET "/" => async(capacity = 1, body = discarded)
//!             common::route::glue::hygiene::keep_nested,
//!     }
//! }
//! sark_fiber::define_task_route! {
//!     TaskApp: sark_fiber::EmptyState => {
//!         GET "/" => async(capacity = 1, body = discarded)
//!             common::route::glue::hygiene::keep_nested,
//!     }
//! }
//! # fn main() {}
//! ```

//! Local state declarations cannot supply a module-scoped generated router.
//!
//! ```compile_fail
//! # mod common;
//! fn main() {
//!     type State = sark_fiber::EmptyState;
//!     sark_fiber::define_router! {
//!         Router: State => {
//!             GET "/" => common::route::glue::agnostic::plain_h,
//!         }
//!     }
//! }
//! ```

use std::env;

use dope_test::checks::{Error, Suite};

#[test]
fn dependent_state() -> Result<(), Error> {
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &[
            "cartel-redis",
            "dope",
            "dope-fiber",
            "dope-gen",
            "dope-quic",
            "dope-test",
            "dope-tls",
            "o3",
            "pin-project",
            "sark",
            "sark-client",
            "sark-core",
            "sark-fiber",
            "sark-grpc",
            "sark-h1",
            "sark-h2",
            "sark-h3",
            "sark-http",
            "sark-json",
        ],
    );
    let exe = env::current_exe()?;
    let output = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.output()?;
    let suite = suite.try_collect(&exe, output)?;
    assert_eq!(
        suite.try_errors(include_str!("sark_fiber_compile_contracts.rs"))?,
        6
    );
    let output = suite.try_compile(include_str!("common/sark_fiber_hrtb.rs"))?;
    assert!(!output.status.success());
    let errors = String::from_utf8(output.stderr).expect("UTF-8 diagnostics");
    assert_eq!(
        errors
            .lines()
            .filter(|line| {
                *line == "error: handler signature higher-ranked lifetimes cannot be named 'req or 'd; choose another name"
            })
            .count(),
        2,
        "{errors}",
    );
    let status = suite.try_rustdoc()?.status()?;
    assert!(
        status.success(),
        "dependent state entry contracts: {status}"
    );
    Ok(())
}
