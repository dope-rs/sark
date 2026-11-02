use std::time;

use dope::{
    core::driver::{
        route::{Identity, KeyTag},
        schedule::ready::wake::queue,
    },
    net::wire::{self, terminal},
    runtime::client,
};
use dope_fiber::{
    abi,
    reply::recv::attach,
    task::time::{budget, sleep},
};
use sark_client::{Error, Failure, capacity, reply};
use sark_http::method::Method;

use crate::client::{Request, Stream, channel, config::report, exchange::Call, process};

/// Copyable HTTP operations scoped to the installed connector application.
pub struct Handle<
    'app,
    'f,
    'e,
    'b,
    'c,
    'w: 'app + 'f,
    'd: 'app + 'f,
    B: ?Sized,
    ID: Identity = KeyTag<0>,
    W: wire::Spec + 'w = wire::Identity,
    F: 'w = report::Never,
> where
    F: report::Policy<terminal::Error<'w, W>>,
{
    pub(super) port: &'app channel::Port<'f, 'e, 'b, 'c, 'w, 'd, ID, W, F>,
    pub(super) bound: client::Bound<'app, 'd, B>,
}

impl<'app, 'f, 'w: 'app + 'f, 'd: 'app + 'f, B: ?Sized, ID: Identity, W: wire::Spec + 'w, F: 'w>
    crate::client::Host<'app> for Handle<'app, 'f, '_, '_, '_, 'w, 'd, B, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>>,
{
    fn host(&self) -> &'app str {
        let requests: &'app _ = &self.port.protocol.requests;
        requests.host()
    }
}

impl<'app, 'f, 'w: 'app + 'f, 'd: 'app + 'f, B: ?Sized, ID: Identity, W: wire::Spec + 'w, F: 'w>
    crate::client::Once<'app, 'd, ID> for Handle<'app, 'f, '_, '_, '_, 'w, 'd, B, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>>,
{
    fn send_once<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        headers: &'r [(&'r str, &'r str)],
        body: &'r [u8],
    ) -> impl abi::Fiber<'d, Output = Result<reply::Scoped<'app, 'd>, Self::Error>> + 'r {
        self.call(method, path, headers, body).scoped::<false>()
    }
}

impl<'app, 'f, 'w: 'app + 'f, 'd: 'app + 'f, B: ?Sized, ID: Identity, W: wire::Spec + 'w, F: 'w>
    crate::client::Ready<'d> for Handle<'app, 'f, '_, '_, '_, 'w, 'd, B, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>>,
{
    type Error = Error;

    fn connections(&self) -> usize {
        self.port.protocol.transport.pool.directory.len()
    }

    fn wait_active<'r>(&'r self) -> impl abi::Fiber<'d, Output = Result<(), Self::Error>> + 'r {
        use std::{pin, task::Poll};

        use dope_fiber::context;

        let net = &self.port.protocol.transport;
        let mut entry = queue::Registration::new();
        abi::poll_fn(move |mut cx: pin::Pin<&mut context::Context<'_, 'd>>| {
            if net.pool.responses.is_exhausted(cx.as_mut().region_token()) {
                entry.clear();
                net.waiters.begin_drain();
                return Poll::Ready(Err(Error::Reply(attach::Error::IdentityExhausted)));
            }
            if net.pool.directory.len() != 0 {
                entry.clear();
                return Poll::Ready(Ok(()));
            }
            if let Some(cause) = self.port.exhausted.get() {
                entry.clear();
                return Poll::Ready(Err(Error::Connect(cause.into())));
            }
            match net.waiters.register_io(&mut entry, cx.as_ref()) {
                Ok(()) => Poll::Pending,
                Err(cause) => {
                    entry.clear();
                    Poll::Ready(Err(Error::Capacity(capacity::Error::Waiter(cause.into()))))
                }
            }
        })
    }
}

impl<'app, 'f, 'w: 'app + 'f, 'd: 'app + 'f, B: ?Sized, ID: Identity, W: wire::Spec + 'w, F: 'w>
    Copy for Handle<'app, 'f, '_, '_, '_, 'w, 'd, B, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>>,
{
}

impl<'app, 'f, 'w: 'app + 'f, 'd: 'app + 'f, B: ?Sized, ID: Identity, W: wire::Spec + 'w, F: 'w>
    Clone for Handle<'app, 'f, '_, '_, '_, 'w, 'd, B, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>>,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'app, 'f, 'w: 'app + 'f, 'd: 'app + 'f, B: ?Sized, ID: Identity, W: wire::Spec + 'w, F: 'w>
    crate::client::Request<'app, 'd, ID> for Handle<'app, 'f, '_, '_, '_, 'w, 'd, B, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>>,
{
    type Error = Failure<F::Detail<'f>>;

    fn send_with<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        headers: &'r [(&'r str, &'r str)],
        body: &'r [u8],
    ) -> impl abi::Fiber<'d, Output = Result<reply::Scoped<'app, 'd>, Self::Error>> + 'r {
        self.call(method, path, headers, body).scoped::<true>()
    }
}

impl<'app, 'f, 'w: 'app + 'f, 'd: 'app + 'f, B: ?Sized, ID: Identity, W: wire::Spec + 'w, F: 'w>
    process::Process<'app, 'd, ID> for Handle<'app, 'f, '_, '_, '_, 'w, 'd, B, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>>,
{
    fn process<'r, T, E, P>(
        &'r self,
        method: Method,
        path: &'r str,
        headers: &'r [(&'r str, &'r str)],
        body: &'r [u8],
        process: P,
    ) -> impl abi::Fiber<'d, Output = Result<Result<T, E>, Self::Error>> + 'r
    where
        P: FnOnce(reply::Scoped<'app, 'd>, budget::Check<'d>) -> Result<T, process::Error<E>> + 'r,
        T: 'r,
        E: 'r,
    {
        self.call(method, path, headers, body).process(process)
    }
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
> Handle<'app, 'f, 'e, 'b, 'c, 'w, 'd, B, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>>,
{
    fn call<'r>(
        &self,
        method: Method,
        path: &'r str,
        headers: &'r [(&'r str, &'r str)],
        body: &'r [u8],
    ) -> Call<'r, Self> {
        Call::new(*self, method, path, headers, body)
    }

    pub fn sleep(&self, duration: time::Duration) -> Result<sleep::Sleep<'d>, sleep::Error> {
        sleep::Sleep::new(self.port.protocol.timer, duration)
    }

    pub fn get<'r>(
        &'r self,
        path: &'r str,
    ) -> impl abi::Fiber<'d, Output = Result<reply::Scoped<'app, 'd>, Failure<F::Detail<'f>>>> + 'r
    {
        self.send(Method::GET, path, &[])
    }

    pub fn send<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        body: &'r [u8],
    ) -> impl abi::Fiber<'d, Output = Result<reply::Scoped<'app, 'd>, Failure<F::Detail<'f>>>> + 'r
    {
        self.send_with(method, path, &[], body)
    }

    pub fn retain<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        body: &'r [u8],
    ) -> impl abi::Fiber<'d, Output = Result<reply::Retained, Failure<F::Detail<'f>>>> + 'r {
        self.call(method, path, &[], body).retained()
    }

    pub fn collect<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        body: &'r [u8],
    ) -> impl abi::Fiber<'d, Output = Result<reply::Contiguous, Failure<F::Detail<'f>>>> + 'r {
        self.collect_with(method, path, &[], body)
    }

    pub fn collect_with<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        headers: &'r [(&'r str, &'r str)],
        body: &'r [u8],
    ) -> impl abi::Fiber<'d, Output = Result<reply::Contiguous, Failure<F::Detail<'f>>>> + 'r {
        self.call(method, path, headers, body).contiguous()
    }

    pub fn stream<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        body: &'r [u8],
    ) -> impl abi::Fiber<
        'd,
        Output = Result<Stream<'app, 'e, 'b, 'c, 'd, B, ID, F::Detail<'f>>, Failure<F::Detail<'f>>>,
    > + 'r {
        self.stream_with(method, path, &[], body)
    }

    pub fn stream_with<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        headers: &'r [(&'r str, &'r str)],
        body: &'r [u8],
    ) -> impl abi::Fiber<
        'd,
        Output = Result<Stream<'app, 'e, 'b, 'c, 'd, B, ID, F::Detail<'f>>, Failure<F::Detail<'f>>>,
    > + 'r {
        self.call(method, path, headers, body).stream()
    }

    /// Returns a transfer-decoded GET event stream.
    /// It does not collect, redirect, or content-decode.
    pub fn stream_get<'r>(
        &'r self,
        path: &'r str,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<Stream<'app, 'e, 'b, 'c, 'd, B, ID, F::Detail<'f>>, Failure<F::Detail<'f>>>,
    > + 'r {
        self.stream(Method::GET, path, &[])
    }
}
