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

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity>
    handle::Scope<'app, 'life, 'e, 'd, P, ID>
{
    fn finish<'scope, 'target, T>(
        self,
        state: &mut State<'d, ID>,
        ready: ready::Target<'d>,
        result: Result<receive::Progress<T>, crate::Status>,
        done: &Notify<'scope, 'life, 'd>,
        wait: Wait<'_, 'target, 'd>,
    ) -> Result<Option<Delivery<'app, 'd, P, T>>, error::Error>
    where
        'life: 'target,
    {
        state.signal(self.connection);
        match result? {
            receive::Progress::Ready(value) => Ok(Some(Delivery::new(value, ready, self.bound))),
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

    pub(in crate::client::manifold) fn poll_unary<'target>(
        self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        call: &unary::Handle<'d, stream::Key>,
        wait: Wait<'_, 'target, 'd>,
    ) -> Result<
        Option<Delivery<'app, 'd, P, receive::UnaryResult<resident::Shared<'d>>>>,
        error::Error,
    >
    where
        'life: 'target,
    {
        self.port.shared.source(token).with(|state, done| {
            let active = state.active_mut(self.connection)?;
            let ready = active.ready;
            let session = &mut active.session.state;
            let result = session
                .poll_unary(call.completion, done)
                .and_then(|result| {
                    if matches!(result, receive::Progress::Pending(_)) {
                        session.ingress.take_error()?;
                    }
                    Ok(result)
                });
            self.finish(state, ready, result, done, wait)
        })
    }

    pub(in crate::client::manifold) fn poll_response<'target>(
        self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        call: &response::Stream<'d, stream::Key>,
        wait: Wait<'_, 'target, 'd>,
    ) -> Result<Option<Delivery<'app, 'd, P, receive::Response<resident::Shared<'d>>>>, error::Error>
    where
        'life: 'target,
    {
        self.port.shared.source(token).with(|state, done| {
            let active = state.active_mut(self.connection)?;
            let ready = active.ready;
            let result = active.session.state.poll_stream(call.completion, done);
            self.finish(state, ready, result, done, wait)
        })
    }
}
