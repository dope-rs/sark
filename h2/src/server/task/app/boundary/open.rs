use std::pin;

use dope::{
    core::driver::{route::Identity, schedule::ready::task::owner},
    manifold::{app::receive, network::listener::conn},
};
use sark_core::{life, manifold::task::app::Runtime};

use crate::server::{
    runtime::output::flush,
    task::{Target, Transport},
};

pub(in crate::server::task::app) fn apply<'d, ID: Identity + 'd, R, M>(
    router: pin::Pin<&mut R>,
    mode: &mut M,
    conn: conn::Id<'d, ID>,
) -> receive::Outcome
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
{
    let owner = owner::Owner::from(conn);
    if let Err(cause) = Runtime::register(router.as_ref(), owner) {
        life::Mode::diagnostic(mode, &cause);
        return receive::Outcome::Failed(receive::Failure::Contract);
    }
    mode.observe(life::Event::SessionOpened(conn));
    receive::Outcome::Continue
}
