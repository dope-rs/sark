use std::{convert, pin};

use dope::{
    core::driver::{
        self,
        route::Identity,
        schedule::{clock::timer, ready::wake::queue},
    },
    manifold::client::{attempt::queue::mailbox, close},
    net::wire::{self, terminal},
};
use dope_fiber::{abi, context, task::time::budget};
use o3::{
    buffer::{resident, segments},
    depot::queue::resident::{Fixed, fifo},
};
use reply::decode::Decoder;
use sark_client::{Error, RequestError, capacity, connect, reply};

use crate::client::{
    config::report,
    dynamic::{self, dial, origin},
    exchange::{
        dispatch::{Admit, Prepared},
        net,
        request::{self, Encoded, Headers, Request},
        response::phases,
    },
    request::Handle,
};

type PreparedFor<'app, 'd, B> = Result<
    Prepared<<B as Backend<'app, 'd>>::Route>,
    Error<<B as Backend<'app, 'd>>::Cause, <B as Backend<'app, 'd>>::Mailbox>,
>;
type Admitted<'app, 'd, B> = Result<
    Admit<
        <B as Backend<'app, 'd>>::Stream,
        <B as Backend<'app, 'd>>::Dial,
        <B as Backend<'app, 'd>>::Route,
    >,
    Error<<B as Backend<'app, 'd>>::Cause, <B as Backend<'app, 'd>>::Mailbox>,
>;

pub(in crate::client) trait Backend<'app, 'd>: Copy + Unpin {
    type Cause;
    type Mailbox: 'd;
    type Stream: phases::Events<'d, Error = Self::Cause> + Unpin;
    type Route: Unpin + 'app;
    type Lanes: 'app;
    type Connected;
    type Dial: abi::Fiber<'d, Output = Result<Self::Connected, connect::Error<Self::Mailbox>>>
        + Unpin;

    fn decoder(self) -> &'app Decoder<'d>;

    fn requests(self) -> &'app Request;

    fn segments(self) -> &'app segments::Pool<resident::Shared<'d>>;

    fn timer(self) -> timer::Timer<'d>;

    fn prepare(
        self,
        request: &request::Admitted<'_>,
        headers: Headers<'_>,
    ) -> PreparedFor<'app, 'd, Self>;

    fn admit<'turn>(
        self,
        prepared: Prepared<Self::Route>,
        check: budget::Check<'d>,
        driver: driver::Context<'turn, 'd>,
    ) -> Admitted<'app, 'd, Self>;

    fn try_register(
        self,
        route: &Self::Route,
        entry: &mut queue::Registration<'app, 'd, Self::Lanes>,
        context: pin::Pin<&context::Context<'_, 'd>>,
    ) -> Result<(), capacity::Error>;

    fn cancel(self, entry: &mut queue::Registration<'app, 'd, Self::Lanes>);

    fn submit<'turn>(
        self,
        ready: Self::Connected,
        encoded: Encoded,
        check: budget::Check<'d>,
        driver: driver::Context<'turn, 'd>,
    ) -> Result<Self::Stream, Error<Self::Cause, Self::Mailbox>>;
}

impl<
    'app,
    'f,
    'e,
    'b,
    'c,
    'w: 'app + 'f,
    'd: 'app + 'f,
    B: ?Sized,
    ID: Identity,
    W: wire::Spec + 'w,
    F: 'w,
> Backend<'app, 'd> for Handle<'app, 'f, 'e, 'b, 'c, 'w, 'd, B, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>>,
{
    type Cause = F::Detail<'f>;
    type Mailbox = convert::Infallible;
    type Stream = crate::client::Stream<'app, 'e, 'b, 'c, 'd, B, ID, Self::Cause>;
    type Route = ();
    type Lanes = Fixed<1>;
    type Connected = convert::Infallible;
    type Dial = abi::Pending<Result<Self::Connected, connect::Error<Self::Mailbox>>>;

    fn decoder(self) -> &'app Decoder<'d> {
        &self.port.protocol.decode
    }

    fn requests(self) -> &'app Request {
        &self.port.protocol.requests
    }

    fn segments(self) -> &'app segments::Pool<resident::Shared<'d>> {
        &self.port.protocol.response_segments
    }

    fn timer(self) -> timer::Timer<'d> {
        self.port.protocol.timer
    }

    fn prepare(
        self,
        request: &request::Admitted<'_>,
        headers: Headers<'_>,
    ) -> PreparedFor<'app, 'd, Self> {
        if !request.is_base() {
            return Err(Error::Request(RequestError::CrossOrigin));
        }
        let encoded = self
            .port
            .protocol
            .requests
            .encode(request, headers)
            .map_err(Error::generalize)?;
        Ok(Prepared::new((), encoded))
    }

    fn admit<'turn>(
        self,
        prepared: Prepared<Self::Route>,
        check: budget::Check<'d>,
        driver: driver::Context<'turn, 'd>,
    ) -> Admitted<'app, 'd, Self> {
        let protocol = &self.port.protocol;
        let Some(claim) = net::Claim::try_acquire(&protocol.transport, driver, (), &protocol.io)?
        else {
            if protocol.transport.pool.directory.len() == 0
                && let Some(cause) = self.port.exhausted.get()
            {
                return Err(Error::Connect(cause.into()));
            }
            return Ok(Admit::Wait(prepared));
        };
        let ((), encoded) = prepared.into_parts();
        crate::client::Stream::submit::<false, _, _, _>(
            &protocol.io,
            self.bound,
            claim,
            encoded,
            check,
        )
        .map(Admit::Stream)
    }

    fn try_register(
        self,
        (): &Self::Route,
        entry: &mut queue::Registration<'app, 'd, Self::Lanes>,
        context: pin::Pin<&context::Context<'_, 'd>>,
    ) -> Result<(), capacity::Error> {
        self.port
            .protocol
            .transport
            .waiters
            .register((), entry, context)
    }

    fn submit<'turn>(
        self,
        ready: Self::Connected,
        _: Encoded,
        _: budget::Check<'d>,
        _: driver::Context<'turn, 'd>,
    ) -> Result<Self::Stream, Error<Self::Cause, Self::Mailbox>> {
        match ready {}
    }

    fn cancel(self, entry: &mut queue::Registration<'app, 'd, Self::Lanes>) {
        self.port.protocol.transport.waiters.cancel(entry);
    }
}

impl<'app, 'o, 'q, 'e, 'b, 'c, 'd: 'app, B: ?Sized, ID: Identity + 'd> Backend<'app, 'd>
    for dynamic::Handle<'app, 'o, 'q, 'e, 'b, 'c, 'd, B, ID>
{
    type Cause = convert::Infallible;
    type Mailbox = mailbox::Error<'d, ID>;
    type Stream = crate::client::Stream<'app, 'e, 'b, 'c, 'd, B, ID>;
    type Route = origin::Key<'o, 'q, 'd, ID>;
    type Lanes = fifo::Scope<'q>;
    type Connected = dial::Connected<'app, 'd, ID>;
    type Dial = dial::Dial<'app, 'o, 'q, 'e, 'b, 'c, 'd, ID>;

    fn decoder(self) -> &'app Decoder<'d> {
        &self.port.protocol.decode
    }

    fn requests(self) -> &'app Request {
        &self.port.protocol.requests
    }

    fn segments(self) -> &'app segments::Pool<resident::Shared<'d>> {
        &self.port.protocol.response_segments
    }

    fn timer(self) -> timer::Timer<'d> {
        self.port.protocol.timer
    }

    fn prepare(
        self,
        request: &request::Admitted<'_>,
        headers: Headers<'_>,
    ) -> PreparedFor<'app, 'd, Self> {
        let is_base = request.is_base();
        let prepare = |origin: &origin::Origin| {
            self.port
                .protocol
                .requests
                .encode_for(origin.authority(), request, headers, is_base)
                .map_err(Error::widen)
        };
        let (origin, encoded) = if is_base {
            self.port.origins.acquire_base(
                self.port.protocol.requests.origin(),
                self.port.protocol.transport.pool.directory.ready,
                &self.port.protocol.transport.waiters,
                prepare,
            )
        } else {
            self.port.origins.acquire(
                request.origin(),
                self.port.protocol.transport.pool.directory.ready,
                &self.port.protocol.transport.waiters,
                prepare,
            )
        }?;
        Ok(Prepared::new(origin, encoded))
    }

    fn admit<'turn>(
        self,
        prepared: Prepared<Self::Route>,
        check: budget::Check<'d>,
        driver: driver::Context<'turn, 'd>,
    ) -> Admitted<'app, 'd, Self> {
        let protocol = &self.port.protocol;
        let (origin, encoded) = prepared.into_parts();
        if let Some(claim) =
            net::Claim::try_acquire(&protocol.transport, driver, origin, &protocol.io)
                .map_err(Error::widen)?
        {
            return crate::client::Stream::submit::<false, _, _, _>(
                &protocol.io,
                self.bound,
                claim,
                encoded,
                check,
            )
            .map(Admit::Stream)
            .map_err(Error::widen);
        }
        match dial::Dial::try_start(self.port, origin).map_err(Error::Connect)? {
            dial::Start::Ready(dial) => Ok(Admit::Dial(dial, encoded)),
            dial::Start::Pending(origin) => Ok(Admit::Wait(Prepared::new(origin, encoded))),
        }
    }

    fn try_register(
        self,
        route: &Self::Route,
        entry: &mut queue::Registration<'app, 'd, Self::Lanes>,
        context: pin::Pin<&context::Context<'_, 'd>>,
    ) -> Result<(), capacity::Error> {
        self.port
            .protocol
            .transport
            .waiters
            .register(*route, entry, context)
    }

    fn submit<'turn>(
        self,
        ready: Self::Connected,
        encoded: Encoded,
        check: budget::Check<'d>,
        driver: driver::Context<'turn, 'd>,
    ) -> Result<Self::Stream, Error<Self::Cause, Self::Mailbox>> {
        let (attempt, conn) = ready.into_parts();
        let protocol = &self.port.protocol;
        let result = match net::Claim::try_new(&protocol.transport, driver, conn) {
            Ok(claim) => crate::client::Stream::submit::<true, _, _, _>(
                &protocol.io,
                self.bound,
                claim,
                encoded,
                check,
            ),
            Err(error) => Err(error),
        };
        match result {
            Ok(stream) => {
                attempt.commit();
                Ok(stream)
            }
            Err(error) => {
                protocol.io.close(
                    conn,
                    close::Reason::Local(close::LocalOrigin::ResourceFailure),
                );
                attempt.cancel();
                Err(error.widen())
            }
        }
    }

    fn cancel(self, entry: &mut queue::Registration<'app, 'd, Self::Lanes>) {
        self.port.protocol.transport.waiters.cancel(entry);
    }
}
