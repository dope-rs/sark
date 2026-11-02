//! Bounded, driver-owned HTTP origins discovered at request time.

use dope::{
    core::driver::route::{Identity, KeyTag},
    manifold::{
        app::env,
        client::{attempt::queue, conn::engine, session::app::State},
        timing,
    },
    net::{tcp, wire},
    runtime::client,
};

use crate::client::conn;

pub mod channel;
pub(in crate::client) mod dial;
mod handle;
pub mod origin;
mod raw;

pub use handle::Handle;

type Machine<'s, 'p, 'd, ID, Port> = engine::Machine<
    'p,
    'd,
    'd,
    ID,
    State<'d, 'd, ID, conn::Connector<'s, ID, Port>, wire::Identity>,
    queue::Control<'s, 'd, tcp::Tcp, ID>,
    env::Bundle<tcp::Tcp, wire::Identity, timing::Balanced>,
>;

#[pin_project::pin_project]
#[derive(dope_gen::Forward)]
#[repr(transparent)]
pub struct Connector<'s, 'p, 'o, 'q, 'e, 'b, 'c, 'd, ID: Identity = KeyTag<0>> {
    #[pin]
    #[forward('d)]
    pub(in crate::client::dynamic) engine:
        Machine<'s, 'p, 'd, ID, channel::Port<'o, 'q, 'e, 'b, 'c, 'd, ID>>,
}

impl<'s, 'p, 'o, 'q, 'e, 'b, 'c, 'd, ID: Identity> client::Provider<'d>
    for Connector<'s, 'p, 'o, 'q, 'e, 'b, 'c, 'd, ID>
{
    type Client<'app>
        = Handle<'app, 'o, 'q, 'e, 'b, 'c, 'd, Self, ID>
    where
        Self: 'app,
        'd: 'app;

    fn provide<'app>(context: client::Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app,
    {
        let (provider, bound) = context.into_parts();
        let port: &'s channel::Port<'o, 'q, 'e, 'b, 'c, 'd, ID> =
            provider.project_ref().engine.application().session().root;
        Handle { port, bound }
    }
}
