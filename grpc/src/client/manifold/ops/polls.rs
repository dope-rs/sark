use dope::core::driver::{
    route::Identity,
    schedule::ready::{
        self,
        wake::{self, table},
    },
};
use o3::{base::cell::region, buffer::resident};
use sark_h2::stream;

use crate::client::{
    call::{response, unary},
    manifold::{
        conn::State,
        error,
        handle::{self, Delivery},
    },
    receive::{self, policies::Notify},
};

type Wait<'a, 'target, 'd> = Option<(
    &'a mut table::Registration<'target, 'd>,
    &'a wake::Waker<'d>,
)>;

pub(in crate::client::manifold) struct Completion<
    'a,
    'app,
    'life,
    'e,
    'd: 'app,
    P: ?Sized + 'app,
    ID: Identity,
> {
    token: &'a mut region::Token<region::Identity<'d, 1>>,
    scope: handle::Scope<'app, 'life, 'e, 'd, P, ID>,
}

impl<'a, 'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity>
    Completion<'a, 'app, 'life, 'e, 'd, P, ID>
{
    pub(in crate::client::manifold) fn new(
        token: &'a mut region::Token<region::Identity<'d, 1>>,
        scope: handle::Scope<'app, 'life, 'e, 'd, P, ID>,
    ) -> Self {
        Self { token, scope }
    }

    fn finish<'scope, 'target, T>(
        scope: handle::Scope<'app, 'life, 'e, 'd, P, ID>,
        state: &mut State<'d, ID>,
        ready: ready::Target<'d>,
        result: Result<receive::Progress<T>, crate::Status>,
        done: &Notify<'scope, 'life, 'd>,
        wait: Wait<'_, 'target, 'd>,
    ) -> Result<Option<Delivery<'app, 'd, P, T>>, error::Error>
    where
        'life: 'target,
    {
        state.signal(scope.connection);
        match result? {
            receive::Progress::Ready(value) => Ok(Some(Delivery::new(value, ready, scope.bound))),
            receive::Progress::Pending(index) => {
                if let Some(error) = state.terminal() {
                    return Err(error);
                }
                if let Some((lease, wake)) = wait {
                    done.try_register(index, lease, wake)
                        .map_err(error::Error::Completion)?;
                }
                Ok(None)
            }
        }
    }

    pub(in crate::client::manifold) fn unary<'target>(
        self,
        call: &unary::Handle<'d, stream::Key>,
        wait: Wait<'_, 'target, 'd>,
    ) -> Result<
        Option<Delivery<'app, 'd, P, receive::UnaryResult<resident::Shared<'d>>>>,
        error::Error,
    >
    where
        'life: 'target,
    {
        let Self { token, scope } = self;
        scope.port.shared.source(token).with(|state, done| {
            let active = state.active_mut(scope.connection)?;
            let ready = active.ready;
            let result = active.session.poll_unary(call, done);
            Self::finish(scope, state, ready, result, done, wait)
        })
    }

    pub(in crate::client::manifold) fn response<'target>(
        self,
        call: &response::Stream<'d, stream::Key>,
        wait: Wait<'_, 'target, 'd>,
    ) -> Result<Option<Delivery<'app, 'd, P, receive::Response<resident::Shared<'d>>>>, error::Error>
    where
        'life: 'target,
    {
        let Self { token, scope } = self;
        scope.port.shared.source(token).with(|state, done| {
            let active = state.active_mut(scope.connection)?;
            let ready = active.ready;
            let result = active.session.poll_response(call, done);
            Self::finish(scope, state, ready, result, done, wait)
        })
    }
}
