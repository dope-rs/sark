use std::convert::Infallible;

use dope::{
    core::driver::route::Identity,
    manifold::network::listener::conn,
    net::wire::{self, open, terminal},
};
use sark_core::manifold::server::app;

use crate::server::runtime::output;

/// A wire failure with its original owner and connection generation.
pub enum Failure<'w, 'd: 'w, W: wire::Spec + 'w, ID: Identity, E = Infallible, B = Infallible> {
    Construction(B),
    Drive {
        id: conn::Id<'d, ID>,
        cause: app::Error<crate::conn::Error, E>,
    },
    Diagnostic {
        id: conn::Id<'d, ID>,
        cause: app::Error<crate::conn::Error, E>,
    },
    Output {
        id: conn::Id<'d, ID>,
        cause: output::Failure,
    },
    Open(open::Error<'w, W>),
    Wire {
        id: conn::Id<'d, ID>,
        cause: terminal::Error<'w, W>,
    },
}
