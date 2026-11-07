use std::{ops::ControlFlow, task};

use dope::{
    core::{driver::route::Identity, io::socket::option},
    manifold::client::{
        attempt::{self, queue},
        conn,
    },
    net::tcp,
};
use dope_fiber::{
    abi::{self, machine},
    context,
};
use o3::depot::arena::slab::Vacancy;
use queue::mailbox::{self, Error};
use sark_client::connect;

use crate::client::dynamic::{channel, origin};

pub(in crate::client) struct Connected<'app, 'd, ID: Identity> {
    attempt: queue::Lease<'app, 'd, tcp::Tcp, ID>,
    connection: conn::Id<'d, ID>,
}

impl<'app, 'd, ID: Identity> Connected<'app, 'd, ID> {
    pub(in crate::client) fn into_parts(
        self,
    ) -> (queue::Lease<'app, 'd, tcp::Tcp, ID>, conn::Id<'d, ID>) {
        (self.attempt, self.connection)
    }
}

pub(in crate::client) struct Dial<'app, 'o, 'q, 'e, 'b, 'c, 'd, ID: Identity> {
    port: &'app channel::Port<'o, 'q, 'e, 'b, 'c, 'd, ID>,
    state: machine::Machine<
        mailbox::Lease<
            'app,
            'd,
            tcp::Tcp,
            origin::Key<'o, 'q, 'd, ID>,
            channel::Outcome<'d, ID>,
            ID,
        >,
    >,
}

pub(in crate::client) enum Start<'app, 'o, 'q, 'e, 'b, 'c, 'd, ID: Identity> {
    Ready(Dial<'app, 'o, 'q, 'e, 'b, 'c, 'd, ID>),
    Pending(origin::Key<'o, 'q, 'd, ID>),
}

impl<'app, 'o, 'q, 'e, 'b, 'c, 'd, ID: Identity> Dial<'app, 'o, 'q, 'e, 'b, 'c, 'd, ID> {
    pub(in crate::client) fn try_start(
        port: &'app channel::Port<'o, 'q, 'e, 'b, 'c, 'd, ID>,
        origin: origin::Key<'o, 'q, 'd, ID>,
    ) -> Result<Start<'app, 'o, 'q, 'e, 'b, 'c, 'd, ID>, connect::Error<Error<'d, ID>>> {
        let vacant = match port.pending.try_reserve() {
            Ok(vacant) => vacant,
            Err(Error::Queue(Vacancy::Full)) => return Ok(Start::Pending(origin)),
            Err(error) => return Err(connect::Error::Mailbox(error)),
        };
        let attempt = vacant.start(
            origin,
            attempt::StreamTarget::new(origin.address(), option::Set::EMPTY),
        );
        Ok(Start::Ready(Self {
            port,
            state: machine::Machine::new(attempt),
        }))
    }
}

impl<ID: Identity> Drop for Dial<'_, '_, '_, '_, '_, '_, '_, ID> {
    fn drop(&mut self) {
        let Some(attempt) = self.state.take() else {
            return;
        };
        if let mailbox::Cancellation::Settled(outcome) = attempt.cancel() {
            self.port.reclaim(outcome);
        }
    }
}

impl<'app, 'o, 'q, 'e, 'b, 'c, 'd, ID: Identity> abi::Fiber<'d>
    for Dial<'app, 'o, 'q, 'e, 'b, 'c, 'd, ID>
{
    type Output = Result<Connected<'app, 'd, ID>, connect::Error<Error<'d, ID>>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, context) = call.into_parts();
        let this = this.get_mut();
        this.state.step(
            |attempt| match attempt.poll(context.as_ref().completion()) {
                mailbox::Poll::Ready(channel::Outcome::Connected(conn), attempt) => {
                    ControlFlow::Break(Ok(Connected {
                        attempt,
                        connection: conn,
                    }))
                }
                mailbox::Poll::Ready(channel::Outcome::Failed(error), attempt) => {
                    attempt.cancel();
                    ControlFlow::Break(Err(error.into()))
                }
                mailbox::Poll::Pending(attempt) => ControlFlow::Continue(attempt),
            },
        )
    }
}
