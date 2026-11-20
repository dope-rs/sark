use std::{
    fs::File,
    io::{self, Write as _},
    net::Ipv4Addr,
    os::fd::AsFd as _,
    time::Instant,
};

use dope_quic::{
    config,
    conn::{
        self, Connection,
        life::{
            accept,
            options::{self, builder},
            tls::clients::Authority,
        },
        output::transmit,
    },
    dope_tls,
    errors::Connect,
    proto::param,
    runtime::routing,
    rustls::{self, pki_types::ServerName},
    wire::packet::cid,
};
use o3::{
    alloc,
    buffer::{resident, write::consume},
};
use sark_h3::manifold;

const CID: [u8; 8] = [0x13, 0x37, 0x13, 0x37, 0x13, 0x37, 0x13, 0x37];

pub fn certificate(error: &rustls::Error) {
    let mut stderr = File::from(
        io::stderr()
            .as_fd()
            .try_clone_to_owned()
            .expect("diagnostic descriptor"),
    );
    writeln!(stderr, "certificate: {error:?}").expect("diagnostic output");
}

type Native<'d> = Connection<0, Vec<u8>, resident::Shared<'d>>;

#[derive(Debug)]
pub enum Transfer {
    Output(transmit::Error),
    Input(conn::Error),
}

pub fn try_send<'d>(
    work: &mut conn::Workspace,
    from: &mut Native<'d>,
    into: &mut Native<'d>,
    now: Instant,
    report: impl Fn(&rustls::Error),
) -> Result<(), Transfer> {
    let (packets, result) = from.with_scope(|mut scope| scope.output().send(now));
    result.map_err(Transfer::Output)?;
    for mut packet in packets {
        into.with_scope(|mut scope| scope.input(work).recv_packet(&mut packet, now, &report))
            .map_err(Transfer::Input)?;
    }
    Ok(())
}

pub fn try_receive<'d>(
    session: &mut manifold::Session<'d>,
    quic: &mut Native<'d>,
) -> Result<(), manifold::Error> {
    quic.with_scope(|mut scope| {
        while let Some(event) = scope.streams().events().poll() {
            session.ingress().ingest(&mut scope, manifold::Notification::from(event))?;
        }
        Ok(())
    })
}

#[derive(Debug)]
pub enum Build {
    Tls(dope_tls::Error),
    Params(param::Error),
    Options(builder::Invalid),
    Id(consume::Error),
    Authority(Connect),
    Accept(Connect),
    Connect(Connect),
    Workspace(alloc::Error),
    Client(Transfer),
    Server(Transfer),
    Ready { client: bool, server: bool },
}
pub struct Pair<'d> {
    pub server: Native<'d>,
    pub client: Native<'d>,
    pub space: conn::Workspace,
}
impl<'d> Pair<'d> {
    pub fn try_config() -> Result<options::Options, Build> {
        let params = param::Set::builder()
            .idle_ms(30_000)
            .initial_data(1 << 20)
            .local_bidi(1 << 20)
            .remote_bidi(1 << 20)
            .stream_uni(1 << 20)
            .bidi_count(8)
            .uni_count(8)
            .try_build()
            .map_err(Build::Params)?;
        options::Options::builder()
            .transport(|path| path.params(params))
            .try_build()
            .map_err(Build::Options)
    }
    pub fn try_new(report: impl Fn(&rustls::Error)) -> Result<Self, Build> {
        Self::try_with(Self::try_config()?, Self::try_config()?, report)
    }
    pub fn try_with(
        accept: options::Options,
        connect: options::Options,
        report: impl Fn(&rustls::Error),
    ) -> Result<Self, Build> {
        let identity = config::raw::Identity::from_seed(&[0x51; 32]).map_err(Build::Tls)?;
        let pubkey = identity.public_key();
        let cid = cid::Id::try_from(CID).map_err(Build::Id)?;
        let config = identity.server().map_err(Build::Tls)?;
        let mut server = accept::Authority::<0>::new(config, accept)
            .map_err(Build::Authority)?
            .accept::<resident::Shared<'d>>(accept::Ids::initial(cid, cid, cid))
            .map_err(Build::Accept)?;
        let client = config::raw::Identity::client(pubkey).map_err(Build::Tls)?;
        let name = ServerName::IpAddress(Ipv4Addr::LOCALHOST.into());
        let mut client = Authority::try_new(client, name)
            .map_err(|error| Build::Tls(error.into()))?
            .connect::<0, Vec<u8>, resident::Shared<'d>>(cid, cid, connect, routing::Plain)
            .map_err(Build::Connect)?;
        let now = Instant::now();
        let mut space = conn::Workspace::try_new().map_err(Build::Workspace)?;
        for _ in 0..3 {
            try_send(&mut space, &mut client, &mut server, now, &report).map_err(Build::Client)?;
            try_send(&mut space, &mut server, &mut client, now, &report).map_err(Build::Server)?;
        }
        let ready = (
            client.view().lifecycle().is_ready(),
            server.view().lifecycle().is_ready(),
        );
        if ready != (true, true) {
            return Err(Build::Ready {
                client: ready.0,
                server: ready.1,
            });
        }
        Ok(Self {
            server,
            client,
            space,
        })
    }
    pub fn try_credit(credit: u64) -> Result<options::Options, Build> {
        let config = Self::try_config()?;
        let params = config
            .params()
            .to_builder()
            .initial_data(credit)
            .remote_bidi(credit)
            .try_build();
        let params = params.map_err(Build::Params)?;
        config
            .into_builder()
            .transport(|options| options.params(params))
            .try_build()
            .map_err(Build::Options)
    }
}
