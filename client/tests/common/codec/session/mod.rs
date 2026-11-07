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
            policy,
            response::{Direct, Immediate},
            turns,
        },
    },
    net::{
        link::{egress, egress::data::Inline, pool::change::opening},
        wire::Identity,
    },
};
use o3::{
    base::cell::region,
    buffer::{self, hold, resident},
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

pub const PREFIX: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: ";
pub const SUFFIX: &[u8] = b"1\r\n\r\nHTTP/1.1 201 Created\r\nContent-Length: 1\r\n\r\nx";
pub const CONTINUE: [u8; 1] = *b"+";

pub struct Session<'a> {
    pub codec: Codec,
    pub audit: &'a Audit,
}

impl<'d> session::Session<'d> for Session<'_> {
    type Codec = Codec;
    type Connect = Full;
    type ConnState = ();
    type Failure = Observed;
    type Receipt = ();
    type Requests = request::Disabled;
    type Send = Inline<1>;

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
        ctx: &mut turns::Context<'_, '_, 'd, (), Inline<1>>,
    ) -> codec::Step<Direct>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        let codec = &self.codec;
        let audit = self.audit;
        let initial = audit.queries.get() == 0;
        codec.bind(storage, state, || {
            let query = audit.queries.get();
            audit.queries.set(query + 1);
            Some(if query == 0 {
                Method::Head
            } else {
                Method::Other
            })
        });
        let step = codec.decode(storage, state, budget, limit, |preview| {
            let index = match preview {
                Ok(Preview::Head { parts, .. }) => match parts.status().as_u16() {
                    200 => Some(0),
                    201 => Some(1),
                    _ => {
                        audit.fail(Failure::UnexpectedEvent);
                        None
                    }
                },
                Ok(Preview::Body { data, .. }) => {
                    if data != b"x" {
                        audit.fail(Failure::UnexpectedEvent);
                    }
                    Some(2)
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
                move |parsed: Result<Parsed<'d>, Error>, state: &mut State<'d>| match parsed {
                    Ok(parsed) => audit.record(index, codec.admit(state, parsed)),
                    Err(error) => audit.fail(Failure::Protocol(error)),
                },
            )
        });
        if initial && audit.queries.get() != 0 {
            let mut parts = ctx.parts();
            if let egress::Enqueue::Rejected(error) = parts
                .sink
                .try_enqueue(parts.driver.region_token(), Inline::from(CONTINUE))
            {
                audit.fail(Failure::Egress(*error.error()));
            }
        }
        step
    }

    fn finish<'slot, 'id, S>(
        &mut self,
        input: Owner<'slot, 'id, Storage<S>, State<'d>>,
        budget: &resident::Budget<'d>,
        limit: Limit,
        _: &mut turns::Context<'_, '_, 'd, (), Inline<1>>,
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

impl<'d> session::Output<'d> for Session<'_> {}

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
        _: turns::Startup<'_, '_, 'd, (), Inline<1>>,
    ) {
        self.audit.connected.set(self.audit.connected.get() + 1);
    }
}

impl<'d> session::Failed<'d> for Session<'_> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: attempt::ConnectFault,
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

    fn io_fail(&mut self, cause: Errno, _: &mut turns::Context<'_, '_, 'd, (), Inline<1>>) {
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
        if self.audit.accepted[2].get() == 0 {
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
