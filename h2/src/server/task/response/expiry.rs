use std::pin;

use dope::{core::driver::route::Identity, manifold::network::listener::conn};
use o3::depot::queue::indexed::lanes::typed::Value;
use sark_core::{deadline, life, manifold::task::app::Runtime};
use sark_http::owner::cause;

use crate::{
    frame::numbers,
    server::task::{
        self, Borrowed, Target, registry,
        response::{Data, Response, deadline::Deadline},
        timers::Due,
    },
    stream,
};

pub(in crate::server::task) struct Expiry(deadline::Poll<Due>);

impl Expiry {
    pub(in crate::server::task) const fn new(due: deadline::Poll<Due>) -> Self {
        Self(due)
    }

    pub(in crate::server::task) fn apply<'d, ID: Identity + 'd, R, Q>(
        self,
        router: pin::Pin<&mut R>,
        tasks: &mut registry::Table<'d, ID, R::Task>,
        conn: conn::Id<'d, ID>,
        index: &mut registry::Index,
        state: &mut Borrowed<'_, 'd, R>,
        mode: &mut Q,
    ) -> Deadline
    where
        R: Runtime<'d, Target = Target<'d, ID>>,
        Q: life::Mode<conn::Id<'d, ID>, stream::Key>,
    {
        let due = match self.0 {
            deadline::Poll::Idle => return Deadline::Ready,
            deadline::Poll::Renewed => return Deadline::Wake,
            deadline::Poll::Expired(due) => due,
        };
        match due {
            Due::Exchange { key, phase } => {
                let xchg = task::identify(conn, key);
                let expiry = xchg.begin_expiry(mode, phase);
                if let Some(registry::Taken { task, .. }) = tasks.take_stream(index, conn, key) {
                    Runtime::cancel(router, task);
                    mode.observe(life::Event::TaskCancelled(xchg));
                }
                let (reply, done) = state.reset(xchg, numbers::ErrorCode::Cancel, mode.body(xchg));
                if let Some(write) = done {
                    task::release_retired(mode, conn, write, cause::Staging::Reset);
                }
                match reply {
                    Some(Value::Data(Data::Body { pending, .. })) => {
                        task::release_pending(mode, xchg, pending, cause::Staging::Reset)
                    }
                    Some(Value::Control(Response::Encoded(receipt))) => drop(receipt),
                    Some(Value::Control(
                        Response::Head { head, .. } | Response::DuplexHead { head, .. },
                    )) => head.discard(state.connection),
                    Some(Value::Data(Data::Duplex { .. }))
                    | Some(Value::Control(
                        Response::Status(_)
                        | Response::Sync(_)
                        | Response::Completion(_)
                        | Response::DuplexOpen { .. },
                    ))
                    | None => {}
                    Some(Value::Idle(never)) => match never {},
                }
                expiry.finish(mode);
                Deadline::Expired
            }
            Due::Wake { .. } => Deadline::Wake,
            Due::Critical | Due::Header { .. } => Deadline::Critical,
        }
    }
}
