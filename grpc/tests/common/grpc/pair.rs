use o3::alloc;
use sark_grpc::{
    Status,
    client::{
        config::{Error, Limits, Output},
        io::ingress::Session as _,
        session::{Create, Session},
    },
    protocol::section,
};
use sark_h2::{
    ServerRole, client,
    conn::{self, config, machine::Connection},
};

#[derive(Debug)]
pub enum Failure {
    Limits(Error),
    Carrier(config::Error),
    Server(config::Error),
    Create(Create<conn::Build, alloc::Error>),
    Ingress(Status),
}

pub struct Pair {
    limits: Limits,
    carrier: client::Plan<section::Plan>,
    server: Connection<ServerRole>,
}

impl Pair {
    pub fn try_flow() -> Result<Self, Failure> {
        let limits = Limits::default()
            .try_calls(2)
            .map_err(Failure::Limits)?
            .try_bytes(8 * 1024)
            .map_err(Failure::Limits)?
            .try_output(Output::try_new(1).map_err(Failure::Limits)?)
            .map_err(Failure::Limits)?;
        let carrier =
            client::Plan::<section::Plan>::new(limits.calls()).map_err(Failure::Carrier)?;
        let server = Connection::<ServerRole>::with_config(config::Config {
            local_settings: config::Settings {
                initial_window_size: 0,
                ..config::Settings::DEFAULT
            },
            ..config::Config::default()
        })
        .map_err(Failure::Server)?;
        Ok(Self {
            limits,
            carrier,
            server,
        })
    }

    pub fn try_run<R>(
        self,
        run: impl for<'scope> FnOnce(
            Session<'scope, client::Plan<section::Plan>>,
            Connection<ServerRole>,
        ) -> R,
    ) -> Result<R, Failure> {
        Session::<_>::with_limits(self.carrier, self.limits, |mut client| {
            let mut server = self.server;
            let settings = server.output().first().to_vec();
            server.transfer().drain(settings.len());
            client.ingest(&settings).map_err(Failure::Ingress)?;
            Ok(run(client, server))
        })
        .map_err(Failure::Create)?
    }
}
