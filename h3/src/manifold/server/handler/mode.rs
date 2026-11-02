use dope_quic::{
    conn::id,
    runtime::mux::handler::{
        hook::{
            life::{close, closing},
            set,
            wire::{
                endpoint::{Off, On},
                ingress, packet,
            },
        },
        net,
    },
    stream::Retain,
    wire::stream::Id,
};
use endpoint::Observer;
use o3::buffer::resident;
use sark_core::{
    life::{self, endpoint, mode},
    manifold::task::app::Runtime,
};
use server::{Handler, deadline, failure, rotation};

use crate::manifold::{owners, server};

/// Selects the native event hooks together with their retained ownership evidence.
pub trait Mode<'d, const ID: u8>:
    life::Mode<id::Handle<id::Driver<'d>, ID>, Id, Retention: owners::Handoff>
    + endpoint::Mode<id::Domain<id::Driver<'d>, ID>, net::Owner<id::Driver<'d>, ID>>
    + failure::Sink<id::Handle<id::Driver<'d>, ID>>
    + Sized
{
    type Events<R, L, E, Q, B>: set::Dispatch<
            Handler<'d, R, Self, L, E, Q>,
            ID,
            B,
            resident::Shared<'d>,
            id::Driver<'d>,
            Established = set::On,
            Stream = set::On,
            Socket = set::On,
            Packet = Q,
            Close = close::On,
        >
    where
        R: Runtime<'d, Target = server::Handle<'d, ID>>,
        L: rotation::Policy<'d>,
        E: deadline::Policy<'d>,
        B: Retain<'d>,
        Q: packet::Mode<Handler<'d, R, Self, L, E, Q>, ID, B, resident::Shared<'d>, id::Driver<'d>>;
}

impl<'d, const ID: u8, O: failure::Sink<id::Handle<id::Driver<'d>, ID>>> Mode<'d, ID>
    for failure::Quiet<O>
{
    type Events<R, L, E, Q, B>
        = set::Set<
        set::On,
        set::Off,
        set::On,
        set::Off,
        set::Off,
        set::On,
        ingress::Off,
        Q,
        Off,
        closing::Off,
        close::On,
    >
    where
        R: Runtime<'d, Target = server::Handle<'d, ID>>,
        L: rotation::Policy<'d>,
        E: deadline::Policy<'d>,
        B: Retain<'d>,
        Q: packet::Mode<Handler<'d, R, Self, L, E, Q>, ID, B, resident::Shared<'d>, id::Driver<'d>>;
}

impl<'d, const ID: u8, O> Mode<'d, ID> for mode::Observed<O>
where
    O: life::Observer<id::Handle<id::Driver<'d>, ID>, Id>
        + Observer<id::Domain<id::Driver<'d>, ID>, net::Owner<id::Driver<'d>, ID>>
        + failure::Sink<id::Handle<id::Driver<'d>, ID>>,
{
    type Events<R, L, E, Q, B>
        = set::Set<
        set::On,
        set::Off,
        set::On,
        set::Off,
        set::On,
        set::On,
        ingress::Off,
        Q,
        On,
        closing::On,
        close::On,
    >
    where
        R: Runtime<'d, Target = server::Handle<'d, ID>>,
        L: rotation::Policy<'d>,
        E: deadline::Policy<'d>,
        B: Retain<'d>,
        Q: packet::Mode<Handler<'d, R, Self, L, E, Q>, ID, B, resident::Shared<'d>, id::Driver<'d>>;
}
