use core::marker;

use sark_core::life::session;
use sark_http::{field::metadata::section::rules, payload::compress::dynamic::hash};

use crate::conn::{
    self, config, event,
    ingress::{self, Config, Fields},
    machine::{Connection, ops::state},
    outputs,
    registry::Registry,
};

impl<R, F, C, A, E> Connection<R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    /// Builds from admitted geometry and an owning hash seed without acquiring entropy.
    pub fn from_seed(config: config::Validated<R>, seed: hash::Seed) -> Result<Self, conn::Build> {
        use conn::Build;

        let config::Validated {
            local,
            recv_window_target: recv,
            streams,
            egress,
            hpack,
            buffers,
            marker: _,
        } = config;
        let streams = Registry::try_new(streams).map_err(Build::Streams)?;
        Ok(Self {
            ingress: ingress::Ingress::try_new(Config {
                capacity: buffers.ingress,
                event_capacity: buffers.event_capacity,
                retention: buffers.retention,
                headers: Fields {
                    plan: buffers.header_plan,
                    decoder: hpack.decoder(),
                    cap: buffers.inbound_header_list_cap,
                    connect: local.enable_connect_protocol,
                },
                preface: ingress::PrefacePhase::for_role::<R>(),
            })
            .map_err(Build::Ingress)?,
            egress: outputs::Egress::try_new::<R>(
                egress,
                buffers.ingress.segments(),
                hpack.encoder(),
                buffers.outbound_headers,
                buffers.header_wire,
                seed,
            )
            .map_err(Build::Egress)?,
            state: state::State::new(local, recv, streams),
            push: marker::PhantomData,
        })
    }
}
