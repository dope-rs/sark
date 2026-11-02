use std::num::NonZero;

use o3::buffer::{pool, segments::queue, storage};
use sark_core::life::session;
use sark_http::{
    self,
    field::metadata::{
        block,
        section::{rules, state::Selected},
    },
    payload::compress::dynamic::{mode, plan},
};

use crate::{
    Endpoint,
    conn::{build, event},
    hpack::decode::mode::Store,
};

type Decode = Store<(), ()>;

pub(crate) mod events;
pub(crate) mod header;
pub(crate) mod headers;
pub(crate) mod input;
pub(super) mod payloads;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrefacePhase {
    Magic,
    InitialSettings,
    Established,
}

pub(super) struct Fields {
    pub(super) plan: pool::Layout,
    pub(super) decoder: plan::Plan<mode::Staged>,
    pub(super) cap: usize,
    pub(super) connect: block::Connect,
}

pub(super) struct Config {
    pub(super) capacity: queue::Capacity,
    pub(super) event_capacity: NonZero<u32>,
    pub(super) retention: payloads::Plan,
    pub(super) headers: Fields,
    pub(super) preface: PrefacePhase,
}

pub(super) struct Ingress<
    P: rules::Policy,
    C: session::Storage = storage::Shared,
    E: event::Evidence = event::Retirement,
> {
    pub(super) input: input::FrameInput<C>,
    pub(super) events: events::Events<Selected<P>, P::Block, C, E>,
    pub(super) headers: headers::Blocks<P>,
    /// The decoder stopped at a resumable capacity boundary. Event ownership
    /// may be drained independently before the next decoder turn.
    pub(super) decoder_blocked: bool,
}

impl<P: rules::Policy, C: session::Storage, E: event::Evidence> Ingress<P, C, E> {
    pub(super) fn try_new(config: Config) -> Result<Self, build::Ingress> {
        use crate::conn::{
            build::Ingress,
            event::Event,
            ingress::{events::Events, headers::Blocks, input::FrameInput},
        };
        let Config {
            capacity,
            event_capacity: events,
            retention: retain,
            headers,
            preface,
        } = config;
        let first = (preface == PrefacePhase::InitialSettings).then_some(Event::PrefaceComplete);
        Ok(Self {
            input: FrameInput::try_new(capacity, retain, preface).map_err(Ingress::Input)?,
            events: Events::try_new(events, first).map_err(Ingress::Events)?,
            headers: Blocks::try_new(headers.plan, headers.decoder, headers.cap, headers.connect)?,
            decoder_blocked: false,
        })
    }
}

impl PrefacePhase {
    pub(super) fn for_role<R: crate::Role>() -> Self {
        match R::ENDPOINT {
            Endpoint::Client => Self::InitialSettings,
            Endpoint::Server => Self::Magic,
        }
    }
}

const _: () = assert!(core::mem::size_of::<PrefacePhase>() == core::mem::size_of::<bool>());
