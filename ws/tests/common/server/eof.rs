use std::{convert::Infallible, net::SocketAddr, pin::Pin};

use dope::{
    core::{
        driver::{
            self,
            route::KeyTag,
            schedule::clock,
            settings::{self, claim, errors},
        },
        io::socket::{Backlog, option},
        random::Seed,
    },
    manifold::{
        ConfigError,
        app::{env::Bundle, receive},
        network::listener::{
            config, conn,
            handler::{self, failures, retire, send},
            topology::factory,
        },
        timing::Throughput,
    },
    net::{
        link::{egress, slot::types::CloseGate},
        tcp,
        wire::{self, domain::Local},
    },
};
use dope_test::alloc::Tracker;
use o3::buffer::{bytes, storage};
use sark_ws::server::{ConnState, app::App};

use crate::common::echo::Echo;

pub fn config(
    bind: SocketAddr,
    max: usize,
    send: option::Buffer,
) -> Result<config::Prepared<tcp::Tcp>, ConfigError> {
    config::Config::<tcp::Tcp> {
        bind,
        max_connections: max,
        per_ip: None,
        backlog: Backlog::fixed::<2>(),
        direct_flights: 1.into(),
        stream: tcp::StreamConfig {
            send_buffer_size: Some(send),
            no_delay: Some(option::Switch::On),
            ..Default::default()
        },
        transport: Default::default(),
        egress: egress::Config::shared(egress::Credit::fixed::<0, 0>()),
    }
    .try_prepare()
}

/// Observes the first completed EOF callback without changing the real app's policy.
pub struct Observed<'a> {
    pub inner: App<Echo<'a>>,
    pub timing: Option<handler::Timing>,
    pub ended: Option<&'a dyn Fn()>,
    pub received: Option<&'a dyn Fn(usize, (usize, usize))>,
}

impl settings::Resource for Observed<'_> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        settings::Resource::include_in(&self.inner, needs)
    }
}

impl factory::ReceiveSpec for Observed<'_> {
    type Input = receive::Coupled;
}

impl factory::Direct<KeyTag<0>, Bundle<tcp::Tcp, wire::Identity, Throughput>> for Observed<'_> {
    type Application<'d>
        = Self
    where
        Self: 'd;
    type Error = Infallible;
    type Wire<'d>
        = Local<'d, wire::Identity, KeyTag<0>>
    where
        Self: 'd;

    fn build<'d>(self) -> Result<(Self, Self::Wire<'d>), Self::Error>
    where
        Self: 'd,
    {
        Ok((self, Local::new(Default::default())))
    }
}

impl<'d> handler::Application<'d, KeyTag<0>> for Observed<'_> {
    type Conn = ConnState;
    type Build = Infallible;
    type Wire = wire::Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = storage::Shared;
    type Input = receive::Coupled;
    type Send = send::Run;

    fn connection(self: Pin<&Self>, seed: &Seed) -> Result<Self::Conn, Self::Build> {
        handler::Application::<KeyTag<0>>::connection(Pin::new(&self.inner), seed)
    }

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn timing(&self) -> handler::Timing {
        self.timing
            .unwrap_or_else(|| handler::Application::<KeyTag<0>>::timing(&self.inner))
    }

    fn accept<'e>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, KeyTag<0>, Self::Wire, Self::Conn, Self::Egress>,
    ) -> receive::Outcome {
        handler::Application::accept(Pin::new(&mut self.get_mut().inner), conn)
    }

    fn timeout<'e>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, KeyTag<0>, Self::Wire, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> handler::Timeout<'d> {
        handler::Application::timeout(Pin::new(&mut self.get_mut().inner), conn, driver)
    }

    fn deadline(self: Pin<&Self>, clock: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        handler::Application::<KeyTag<0>>::deadline(Pin::new(&self.inner), clock)
    }

    fn close_gate(
        self: Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, KeyTag<0>, Self::Wire, Self::Conn, Self::Egress>,
    ) -> CloseGate {
        handler::Application::close_gate(Pin::new(&self.inner), conn)
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, KeyTag<0>, Self::Wire, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, KeyTag<0>, Self::Wire, Self::Conn, Self::Egress>
    where
        'd: 'w,
    {
        handler::Application::activate(Pin::new(&mut self.get_mut().inner), conn, driver)
    }

    fn eof<'e>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, KeyTag<0>, Self::Wire, Self::Conn, Self::Egress>,
        end: receive::End,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let this = self.get_mut();
        handler::Application::eof(Pin::new(&mut this.inner), conn, end, driver);
        if let Some(ended) = this.ended.take() {
            ended();
        }
    }
}

impl<'d> handler::Borrowed<'d, KeyTag<0>> for Observed<'_> {
    fn chunk<R: bytes::TryRetain>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, KeyTag<0>, Self::Wire, Self::Conn, Self::Egress>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> receive::Outcome {
        let this = self.get_mut();
        let Some(received) = this.received else {
            return handler::Borrowed::chunk(Pin::new(&mut this.inner), conn, chunk, driver);
        };
        let length = chunk.as_ref().len();
        let (outcome, allocations) = Tracker::usage(|| {
            handler::Borrowed::chunk(Pin::new(&mut this.inner), conn, chunk, driver)
        });
        received(length, allocations);
        outcome
    }
}

impl<'d> send::Drive<'d, KeyTag<0>> for Observed<'_> {
    fn send(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, KeyTag<0>, Self::Wire, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        send::Drive::send(Pin::new(&mut self.get_mut().inner), conn, driver);
    }
}
