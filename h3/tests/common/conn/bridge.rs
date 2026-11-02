use std::{
    fs::File,
    io::{self, Write as _},
    net::Ipv4Addr,
    os::fd::AsFd as _,
    time::Instant,
};

use dope_quic::{
    config, conn,
    conn::{
        Connection,
        life::{
            accept,
            options::{self, builder},
            tls::clients::Authority,
        },
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

use crate::common::conn::wire::{Peer, Transfer};

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
    pub server: Connection<0, Vec<u8>, resident::Shared<'d>>,
    pub client: Connection<0, Vec<u8>, resident::Shared<'d>>,
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
            Peer::new(&mut space, &mut client)
                .try_send(&mut server, now, &report)
                .map_err(Build::Client)?;
            Peer::new(&mut space, &mut server)
                .try_send(&mut client, now, &report)
                .map_err(Build::Server)?;
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
