use std::convert::Infallible;

use dope::{
    core::{Errno, driver, driver::route::KeyTag, io::socket},
    manifold::client::{
        app::{admit::Automatic, connect::Full, failure::Observed, request},
        attempt,
        close::{self, Policy},
        conn,
        session::{
            self, Blocking,
            app::drain::Ready,
            codec::{self, Admission, Stateful as _, mode, staging::Limit, storage::Owner},
            policy::{self, output},
            response::{Direct, Immediate},
            turns,
        },
    },
    net::{
        link::{egress, event, pool::change::opening},
        wire::Identity,
    },
};
use o3::{
    base::{Call, cell::region},
    buffer::{self, hold, resident, storage::Shared},
};
use sark_client::protocol::{
    Error,
    codec::{
        Codec, State, Storage,
        output::{Parsed, Preview},
    },
};
use sark_http::payload::response::Method;

mod audit;
pub use audit::Audit;
mod failure;
pub use failure::Failure;

pub struct Session<'a> {
    pub codec: Codec,
    pub audit: &'a Audit,
}

impl<'d> session::Session<'d> for Session<'_> {
    type Codec = Codec;
    type Connect = Full;
    type ConnState = ();
    type Failure = Observed;
    type Requests = request::Disabled;
    type Send = Shared;

    fn codec(&self) -> &Codec {
        &self.codec
    }
}

impl<'d> session::Parser<'d> for Session<'_> {
    type Mode = mode::Stored;
    type Response = Immediate;
}

impl<'d> session::Stored<'d> for Session<'_> {
    type Delivery = Direct;

    fn blocked(&self, _: conn::Id<'d, KeyTag<0>>, _: &mut State<'d>) -> Blocking {
        self.audit.blocked.set(self.audit.blocked.get() + 1);
        Blocking::Unsupported
    }

    fn decode<S>(
        &mut self,
        _: conn::Id<'d, KeyTag<0>>,
        storage: &mut Storage<S>,
        state: &mut State<'d>,
        budget: &resident::Budget<'d>,
        limit: Limit,
        ctx: &mut turns::Context<'_, '_, 'd, (), Shared>,
    ) -> codec::Step<Direct>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        let codec = &self.codec;
        let audit = self.audit;
        codec.decode(storage, state, budget, limit, |preview| {
            let index = match preview {
                Ok(Preview::Head { parts, .. }) => {
                    if parts.status().as_u16() != 200 {
                        audit.fail(Failure::UnexpectedEvent);
                    }
                    Some(0)
                }
                Ok(Preview::Body { data, .. }) => {
                    if data != b"x" {
                        audit.fail(Failure::UnexpectedEvent);
                    }
                    Some(1)
                }
                Err(_) => None,
            };
            if let Some(index) = index {
                let attempt = audit.attempts[index].get();
                audit.attempts[index].set(attempt + 1);
                if attempt == 0 {
                    ctx.wake_target().wake();
                    return Admission::RetryInput;
                }
            }
            Admission::Ready(
                move |parsed: Result<Parsed<'d>, Error>, state: &mut State<'d>| {
                    let parsed = match parsed {
                        Ok(parsed) => parsed,
                        Err(error) => {
                            audit.fail(Failure::Protocol(error));
                            return;
                        }
                    };
                    let output = codec.admit(state, parsed, || Some(Method::Other));
                    audit.record(index, output);
                },
            )
        })
    }

    fn finish<'slot, 'id, S>(
        &mut self,
        input: Owner<'slot, 'id, Storage<S>, State<'d>>,
        budget: &resident::Budget<'d>,
        limit: Limit,
        _: &mut turns::Context<'_, '_, 'd, (), Shared>,
    ) -> (
        Owner<'slot, 'id, Storage<S>, State<'d>>,
        Result<codec::Finish<Direct>, Direct>,
    )
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        let audit = self.audit;
        self.codec.finish(input, budget, limit, |_| {
            Admission::Ready(
                move |parsed: Result<Parsed<'d>, Error>, _: &mut State<'d>| match parsed {
                    Err(error) => audit.fail(Failure::Protocol(error)),
                    Ok(_) => audit.fail(Failure::UnexpectedEvent),
                },
            )
        })
    }
}

impl<'d> session::Output<'d> for Session<'_> {
    fn flush()
    -> impl for<'a, 'c, 't, 'e> Call<(&'a mut Self, &'c mut turns::Context<'t, 'e, 'd, (), Shared>)>
    {
        output::Flush::<Infallible>::None
    }
}

impl<'d> policy::Compatible<'d, 'd, Identity, KeyTag<0>> for Session<'_> {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self {
        self
    }

    fn terminal(&self) -> &Self {
        self
    }
}

impl<'d> session::Connected<'d> for Session<'_> {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        _: turns::Startup<'_, '_, 'd, (), Shared>,
    ) {
        self.audit.connected.set(self.audit.connected.get() + 1);
    }
}

impl<'d> session::Failed<'d> for Session<'_> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.fail(Failure::Connect(cause));
    }
}

impl<'d> session::Exhausted<'d> for Session<'_> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.fail(Failure::Exhausted(cause));
    }
}

impl<'d> session::Retirement<'d> for Session<'_> {
    type Drain = Ready;

    fn io_fail(&mut self, cause: Errno, _: &mut turns::Context<'_, '_, 'd, (), Shared>) {
        self.audit.fail(Failure::Io(cause));
    }

    fn quarantine(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut (),
        reason: close::Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        if self.audit.accepted[1].get() == 0 {
            self.audit.fail(Failure::Closed(reason));
        }
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn policy(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &(),
        _: close::Reason,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> Policy {
        Policy::Permanent
    }

    fn disconnect(&mut self, _: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        self.audit.retired.set(self.audit.retired.get() + 1);
    }
}

impl<'d> session::Scheduling<'d> for Session<'_> {}
impl<'d> session::Target<'d, KeyTag<0>, 1> for Session<'_> {}
