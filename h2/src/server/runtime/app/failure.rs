use std::{convert::Infallible, fmt};

use crate::server::app;
use dope::{
    core::driver::route::Identity,
    manifold::network::listener::conn,
    net::wire::{self, open, terminal},
};

use crate::server::runtime::output;

/// A failure preserving its wire owner or borrowing its response diagnostic.
pub enum Failure<'a, 'w, 'd: 'w, W: wire::Spec + 'w, ID: Identity, E = Infallible, B = Infallible> {
    Construction(B),
    Drive {
        id: conn::Id<'d, ID>,
        cause: app::Error<E>,
    },
    Diagnostic {
        id: conn::Id<'d, ID>,
        cause: app::Error<E>,
    },
    Output {
        id: conn::Id<'d, ID>,
        cause: output::Failure,
    },
    Response {
        id: conn::Id<'d, ID>,
        stream: crate::stream::Key,
        cause: &'a dyn fmt::Debug,
    },
    Open(open::Error<'w, W>),
    Wire {
        id: conn::Id<'d, ID>,
        cause: terminal::Error<'w, W>,
    },
}
