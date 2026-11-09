use std::pin;

use dope::{core::driver::route::Identity, manifold::network::listener::conn};
use sark_core::{deadline, life, manifold::task::app::Runtime};
use sark_http::{field::metadata::section::rules, owner::cause};

use crate::{
    frame::numbers,
    server::{
        state::Transitions,
        task::{
            self, Borrowed, Target, ingress, registry,
            response::{self, deadline::Deadline},
            timers::Due,
        },
    },
};

pub(in crate::server::task) fn apply<'d, ID: Identity + 'd, R, Q, F>(
    due: deadline::Poll<Due>,
    router: pin::Pin<&mut R>,
    tasks: &mut registry::Table<'d, ID, R::Task>,
    conn: conn::Id<'d, ID>,
    index: &mut registry::Index,
    state: &mut Borrowed<'_, 'd, R, F>,
    mode: &mut Q,
) -> Deadline
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    Q: Transitions<conn::Id<'d, ID>>,
    F: rules::Policy,
{
    let due = match due {
        deadline::Poll::Idle => return Deadline::Ready,
        deadline::Poll::Renewed => return Deadline::Wake,
        deadline::Poll::Expired(due) => due,
    };
    match due {
        Due::Exchange { key, phase } => {
            let xchg = task::identify(conn, key);
            let mut expiry = None;
            if let Some((_, task, unreported)) = tasks.take_stream(index, key) {
                expiry = Some(xchg.begin_expiry(mode, phase));
                Runtime::cancel(router, task);
                if unreported {
                    mode.observe(life::Event::TaskCancelled(xchg));
                }
            }
            let discarded = {
                let expiry = &mut expiry;
                let mode = &mut *mode;
                state.data().discard(xchg, move || {
                    expiry.get_or_insert_with(|| xchg.begin_expiry(mode, phase));
                    mode.body(xchg)
                })
            };
            let code = match discarded {
                ingress::Discard::Missing(_) | ingress::Discard::Removed(_) => {
                    numbers::ErrorCode::Cancel
                }
                ingress::Discard::Failed { error, .. } => error.code(),
            };
            let reply = state.responses.remove(key);
            if reply.is_some() {
                expiry.get_or_insert_with(|| xchg.begin_expiry(mode, phase));
            }
            let done = if expiry.is_some() {
                state.connection.streams().retire(key, code)
            } else {
                mode.cancel(key);
                state.connection.cancel_ingress(key, code)
            };
            if let Some(write) = done {
                task::release_retired(mode, conn, write, cause::Staging::Reset);
            }
            if let Some(reply) = reply {
                response::discard(reply, state.connection, xchg, mode, cause::Staging::Reset);
            }
            if let Some(expiry) = expiry {
                expiry.finish(mode);
            }
            Deadline::Expired
        }
        Due::Critical => Deadline::Critical,
    }
}
