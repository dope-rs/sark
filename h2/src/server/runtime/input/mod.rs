use dope::{
    core::driver::route::Identity,
    manifold::network::listener::conn::Ctx,
    net::{
        link::egress::{self, data},
        wire,
    },
};
use sark_core::manifold::server::app;

use crate::{conn, server::runtime::output};

mod read;
mod turn;

pub(in crate::server) use read::Read;
pub(in crate::server) use turn::Turn;

pub(in crate::server) fn pending<'d, ID, W, C, B>(
    connection: &mut Ctx<'_, '_, '_, '_, '_, 'd, ID, W, C, B>,
    error: &impl Fault,
) -> bool
where
    ID: Identity,
    W: wire::Spec,
    C: output::State<'d>,
    B: data::raw::Payload<'d>,
{
    if !error.connection().is_some_and(conn::Error::is_pressure) || connection.is_closing() {
        return false;
    }
    let protocol = connection.state_mut().protocol();
    protocol.ingress().is_blocked()
        && (!protocol.output().is_empty()
            || matches!(connection.write().pending(), egress::Pending::Present))
}

pub(in crate::server) trait Fault: From<conn::Error> {
    fn connection(&self) -> Option<&conn::Error>;
}

impl Fault for conn::Error {
    fn connection(&self) -> Option<&conn::Error> {
        Some(self)
    }
}

impl<E> Fault for app::Error<conn::Error, E> {
    fn connection(&self) -> Option<&conn::Error> {
        match self {
            Self::Connection(error) => Some(error),
            Self::Application(_) => None,
        }
    }
}
