use std::{
    cell::Cell,
    fmt,
    fs::File,
    io::{self, Write},
    iter,
    net::SocketAddr,
    os::fd::AsFd as _,
    process::ExitCode,
};

use clap::{Arg, Command, error::ErrorKind};

use dope::{
    core::{
        driver::{
            life::recovery::{Policy, Recovery},
            route::table::conn,
            settings::{self, errors::Claim},
        },
        io::{
            Reclaimed,
            socket::{self, option},
        },
        platform::affinity::Cpu,
    },
    manifold::{app::env::quota, network::listener::config},
    net::tcp,
    runtime::{
        executor::session::Completion,
        process::{
            self, Context,
            failure::execute::{Fault, Worker},
        },
        shutdown,
    },
};
use sark_h2::server::{Config, ConfigError, Server, runtime, service::Request};
use sark_http::{
    field,
    payload::{
        fixed::Body,
        response::{headers, statics, status::Code},
    },
};

static LARGE: [u8; 1 << 20] = [b'x'; 1 << 20];

#[derive(Clone, Copy)]
struct Settings {
    bind: SocketAddr,
    h2: Config,
}

#[derive(Debug)]
enum Setup {
    Config(ConfigError),
    Claim(Claim),
    Output(io::Error),
    Run,
}

fn echo<'a>(request: Request<'_, '_, '_>) -> statics::Direct<'a, [headers::Entry<'a>; 1]> {
    let fields = [headers::Entry::new(
        field::literal!(headers::Name::try_bytes(b"content-type")),
        field::literal!(field::Value::try_bytes(b"text/plain; charset=utf-8")),
    )];
    let body = if request.path() == Some(b"/large".as_slice()) {
        Body::new(&LARGE)
    } else {
        Body::new(b"hello from sark-h2\n")
    };
    statics::Direct::new(Code::OK, fields, body)
}

impl Settings {
    fn run<'run>(
        self,
        context: Context<'run>,
    ) -> Result<shutdown::Requested<process::Shutdown<'run>>, Setup> {
        let plan = runtime::Plan::new(
            config::Prepared {
                bind: self.bind,
                connections: conn::Capacity::fixed::<1024>(),
                per_ip: Some(quota::PerIp::fixed::<512>()),
                direct_flights: Default::default(),
                backlog: socket::Backlog::fixed::<256>(),
                stream: tcp::StreamConfig {
                    no_delay: Some(option::Switch::On),
                    ..Default::default()
                },
                transport: tcp::Config::for_tcp(Some(socket::Reuse::Port), None),
                egress: Default::default(),
            },
            settings::Receive::fixed::<1024, { 64 << 10 }>(),
            self.h2,
        )
        .map_err(Setup::Config)?;
        let mut finish = Completion::new();
        let output = Cell::new(None);
        let report = |message: fmt::Arguments<'_>| {
            if let Err(error) = write(message) {
                output.set(output.take().or(Some(error)));
            }
        };
        let result = Server::from_fn(echo, plan)
            .prepare()
            .map_err(Setup::Claim)?
            .run(
                context,
                &mut finish,
                Policy::new(
                    |reclaimed: Reclaimed| {
                        report(format_args!("reclaimed completion: {reclaimed:?}"))
                    },
                    |failure: Recovery<'_, '_>| {
                        report(format_args!(
                            "echo_server: native retirement: {:?}",
                            failure.cause()
                        ));
                        failure.abort()
                    },
                ),
                |error| {
                    report(format_args!("echo_server: {:?}", error.cause()));
                    Err(Setup::Run)
                },
            );
        match output.take() {
            Some(error) => Err(Setup::Output(error)),
            None => result,
        }
    }
}

fn write(message: fmt::Arguments<'_>) -> io::Result<()> {
    let mut stderr = File::from(io::stderr().as_fd().try_clone_to_owned()?);
    writeln!(stderr, "{message}")
}

fn main() -> ExitCode {
    run().unwrap_or(ExitCode::FAILURE)
}

fn run() -> io::Result<ExitCode> {
    let bind = match Command::new("server")
        .arg(
            Arg::new("bind")
                .long("bind")
                .value_parser(str::parse::<SocketAddr>)
                .default_value("127.0.0.1:18080"),
        )
        .try_get_matches()
        .and_then(|mut args| {
            args.try_remove_one::<SocketAddr>("bind")
                .map_err(|error| clap::Error::raw(ErrorKind::InvalidValue, error))?
                .ok_or_else(|| {
                    clap::Error::raw(ErrorKind::MissingRequiredArgument, "--bind is required")
                })
        }) {
        Ok(bind) => bind,
        Err(error) => {
            let status = ExitCode::from(error.exit_code() as u8);
            error.print()?;
            return Ok(status);
        }
    };
    let config = Settings {
        bind,
        h2: Config {
            max_request_body_bytes: 16 << 20,
            max_request_body_chunks: 4 << 10,
            max_connection_body_bytes: 64 << 20,
            max_connection_body_chunks: 16 << 10,
            max_outbound_bytes: 64 << 10,
        },
    };
    write(format_args!("echo_server: listening on {bind}"))?;
    let cpus = match Cpu::current() {
        Ok(cpus) => cpus,
        Err(error) => {
            write(format_args!("echo_server: CPU affinity: {error:?}"))?;
            return Ok(ExitCode::FAILURE);
        }
    };
    let workers = cpus.len();
    let layout = match process::Layout::pinned(cpus) {
        Ok(layout) => layout,
        Err(error) => {
            write(format_args!("echo_server: process creation: {error:?}"))?;
            return Ok(ExitCode::FAILURE);
        }
    };
    let runtime = match layout.try_assign(iter::repeat_n(config, workers)) {
        Ok(runtime) => runtime,
        Err(error) => {
            write(format_args!("echo_server: process creation: {error:?}"))?;
            return Ok(ExitCode::FAILURE);
        }
    };
    let mut signal = std::mem::MaybeUninit::uninit();
    match runtime.run(
        dope::core::o3::mem::slot::Vacant::new(&mut signal),
        Settings::run,
    ) {
        Ok(()) => Ok(ExitCode::SUCCESS),
        Err(error) => error.fold(Ok(ExitCode::FAILURE), |status, fault| {
            let status = status?;
            match fault {
                Fault::Worker(Worker::Entry {
                    cpu,
                    source,
                    bus,
                    handoff,
                }) => {
                    write(format_args!(
                        "echo_server: worker {cpu:?}: bus={bus:?}, handoff={handoff:?}"
                    ))?;
                    match source {
                        Setup::Config(error) => write(format_args!("echo_server: configuration: {error:?}")),
                        Setup::Claim(error) => write(format_args!("echo_server: driver admission: {error:?}")),
                        Setup::Output(error) => write(format_args!("echo_server: diagnostic output: {error}")),
                        Setup::Run => write(format_args!("echo_server: worker run failed")),
                    }
                }
                fault => write(format_args!("echo_server: process: {fault:?}")),
            }?;
            Ok(status)
        }),
    }
}
