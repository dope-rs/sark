use dope::{
    core::driver::route::Identity,
    manifold::{
        app::env,
        network::listener::{self, handler},
    },
};

use crate::server::{date, listener::bind::Bind};

/// Installed listener and its independently stored Date tick.
#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
#[doc(hidden)]
pub struct Root<'n, 'p, 's, 'w, 'd: 'w, ListenerId: Identity, DateId: Identity, A, E>
where
    A: handler::Application<'d, ListenerId, Wire = E::Wire>
        + handler::Runtime<'w, 'd, ListenerId>,
    E: env::Spec,
{
    #[pin]
    #[manifold]
    pub(super) listener: listener::Listener<'n, 'p, 's, 'w, 'd, ListenerId, A, E>,
    #[pin]
    #[app]
    pub(super) date: date::Root<'w, 'd, DateId>,
}

pub(super) type Bound<'n, 'p, 's, 'w, 'd, ID, Date, F, D, E> =
    Root<'n, 'p, 's, 'w, 'd, ID, Date, <F as Bind<'w, 'd, ID>>::Application<'w, D>, E>;
