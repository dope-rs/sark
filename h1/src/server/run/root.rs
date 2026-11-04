use dope::{
    core::driver::route::Identity,
    manifold::{
        app::env,
        network::listener::{self, handler},
    },
    runtime::control,
};
use sark_core::manifold::timer;

use crate::server::{date, listener::bind::Bind, run::Watch};

/// Installed listener and its independently stored Date tick.
#[pin_project::pin_project(PinnedDrop)]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
#[coordinate]
#[doc(hidden)]
pub struct Root<'f, 'n, 'p, 's, 'w, 'd: 'w + 'f, ListenerId: Identity, DateId: Identity, A, E>
where
    A: handler::Application<'d, ListenerId, Wire = E::Wire>
        + handler::Runtime<'w, 'd, ListenerId>
        + timer::Source<'d>,
    E: env::Spec,
{
    #[pin]
    #[manifold(control)]
    pub(super) listener: listener::Listener<'n, 'p, 's, 'w, 'd, ListenerId, A, E>,
    #[pin]
    #[app]
    pub(super) date: date::Root<'w, 'd, DateId>,
    #[dispatcher(state)]
    pub(super) watch: &'f Watch<'d>,
}

#[pin_project::pinned_drop]
impl<'f, 'n, 'p, 's, 'w, 'd: 'w + 'f, ID: Identity, Date: Identity, A, E> PinnedDrop
    for Root<'f, 'n, 'p, 's, 'w, 'd, ID, Date, A, E>
where
    A: handler::Application<'d, ID, Wire = E::Wire>
        + handler::Runtime<'w, 'd, ID>
        + timer::Source<'d>,
    E: env::Spec,
{
    fn drop(self: std::pin::Pin<&mut Self>) {
        let root = self.as_ref().get_ref();
        // Dispatch or child pre-park can fail before another coordination step.
        if let Some(cause) = root.listener.accept_error() {
            root.watch.record(cause);
        }
    }
}

impl<'f, 'n, 'p, 's, 'w, 'd: 'w + 'f, ID: Identity, Date: Identity, A, E>
    Root<'f, 'n, 'p, 's, 'w, 'd, ID, Date, A, E>
where
    A: handler::Application<'d, ID, Wire = E::Wire>
        + handler::Runtime<'w, 'd, ID>
        + timer::Source<'d>,
    E: env::Spec,
{
    pub fn watch(&self) -> &'f Watch<'d> {
        self.watch
    }

    fn coordinate(
        fields: root_coord::Fields<'_, 'f, 'n, 'p, 's, 'w, 'd, ID, Date, A, E>,
    ) -> control::Flow {
        if let Some(cause) = fields.listener.accept_error() {
            fields.watch.record(cause);
        }
        control::Flow::Idle
    }
}

pub(super) type Bound<'f, 'n, 'p, 's, 'w, 'd, ID, Date, F, D, E> =
    Root<'f, 'n, 'p, 's, 'w, 'd, ID, Date, <F as Bind<'w, 'd, ID>>::Application<'w, D>, E>;
