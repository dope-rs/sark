use std::{
    convert, marker,
    process::{ExitCode, Termination},
    time::Duration,
};

use dope::core::{
    BranchError,
    driver::{route::KeyTag, settings::errors::Claim},
    platform::affinity::Cpu,
};
use sark_h1::server::run::prepare;

mod error;
use error::Error;
pub mod input;
mod worker;

use dope::{
    core::driver::route::table::conn,
    runtime::{process, shutdown},
};
use sark::http::payload::response::{
    dispatch::{Stream, chunk::Chunk},
    headers::{set, value::Item},
};
use sark_fiber::{
    Throughput,
    fiber::{IntoStream as _, IterStream},
};
use sark_h1::server::{listener, run};
use sark_http::field::{self, Value};

#[sark::request]
struct Request {
    #[path]
    pub n: usize,
}

const MAX_CONNECTIONS: usize = 1024;
const HTTP_LISTENER_ID: u8 = 0;
const DATE_UPDATER_ID: u8 = 1;

struct Server {
    plan: run::Plan<KeyTag<HTTP_LISTENER_ID>, KeyTag<DATE_UPDATER_ID>, Throughput>,
}

type Setup = BranchError<BranchError<prepare::Error, Claim>, worker::Error>;

impl Server {
    fn run<'run>(
        self,
        context: process::Context<'run>,
    ) -> Result<shutdown::Requested<process::Shutdown<'run>>, Setup> {
        let output = std::cell::Cell::new(None);
        DownloadApp::serve(
            self.plan,
            context,
            (),
            sark_fiber::Config::new(MAX_CONNECTIONS),
            worker::observe(&output),
            |result| worker::finish_run(result, &output),
        )
        .map_err(Setup::First)?
        .map_err(Setup::Second)
    }
}

struct Chunks<'d> {
    remaining: usize,
    marker: marker::PhantomData<&'d ()>,
}

impl<'d> Iterator for Chunks<'d> {
    type Item = Chunk<'d>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        Some(Chunk::from(b"chunk\n"))
    }
}

#[sark::handler]
fn download(request: Request) -> Stream<'req, IterStream<Chunks<'d>>, 1> {
    Chunks {
        remaining: request.n.min(10_000),
        marker: marker::PhantomData,
    }
    .into_stream()
    .headers(set::Set::from_items([Item::borrow(
        sark::header_name!("content-type"),
        field::literal!(Value::try_bytes(b"text/plain; charset=utf-8")),
    )]))
}

sark::define_route! {
    DownloadApp: () => {
        GET "/download/:n" => request(path) stream(capacity = MAX_CONNECTIONS) download,
    }
}

fn start<'signal>(
    signal: o3::mem::slot::Vacant<'signal, dope::core::platform::signal::Receiver>,
) -> Result<(), Error<'signal, convert::Infallible, Setup>> {
    let input::Bind { bind } = input::Bind::try_parse().map_err(Error::Input)?;

    let server = listener::Config::<KeyTag<HTTP_LISTENER_ID>, Throughput>::tcp(
        bind,
        conn::Capacity::fixed::<MAX_CONNECTIONS>(),
        Duration::from_secs(10),
    );

    error::write(format_args!(
        "sark download example: GET http://{bind}/download/<n>"
    ))
    .map_err(Error::Output)?;

    let plan = server.plan::<KeyTag<DATE_UPDATER_ID>>();
    let cpus = Cpu::current().map_err(Error::Affinity)?;
    let layout = process::Layout::pinned(cpus.into_iter().take(1)).map_err(Error::Create)?;
    layout
        .try_assign([Server { plan }])
        .map_err(Error::Create)?
        .run(signal, Server::run)
        .map_err(Error::Run)
}

fn main() -> ExitCode {
    let mut signal = std::mem::MaybeUninit::uninit();
    match start(o3::mem::slot::Vacant::new(&mut signal)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => error.report(),
    }
}
