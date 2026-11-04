use std::{convert::Infallible, num::NonZero, time};

use dope::{
    core::driver::route::{Identity, KeyTag},
    manifold::client::attempt::queue::mailbox,
    runtime::client,
};
use dope_fiber::{
    abi,
    task::time::{budget, sleep},
};
use sark_client::{Failure, reply};
use sark_http::method::Method;

use crate::client::{Request, dynamic::channel, exchange::Call, process};

/// Copyable HTTP operations scoped to one installed on-demand connector.
pub struct Handle<'app, 'o, 'q, 'e, 'b, 'c, 'd: 'app, B: ?Sized, ID: Identity = KeyTag<0>> {
    pub(in crate::client) port: &'app channel::Port<'o, 'q, 'e, 'b, 'c, 'd, ID>,
    pub(in crate::client) bound: client::Bound<'app, 'd, B>,
}

impl<B: ?Sized, ID: Identity> Copy for Handle<'_, '_, '_, '_, '_, '_, '_, B, ID> {}

impl<B: ?Sized, ID: Identity> Clone for Handle<'_, '_, '_, '_, '_, '_, '_, B, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'app, 'd: 'app, B: ?Sized, ID: Identity> crate::client::Host<'app>
    for Handle<'app, '_, '_, '_, '_, '_, 'd, B, ID>
{
    fn host(&self) -> &'app str {
        let requests: &'app _ = &self.port.protocol.requests;
        requests.host()
    }
}

impl<'app, 'o, 'q, 'e, 'b, 'c, 'd: 'app, B: ?Sized, ID: Identity + 'd>
    crate::client::Request<'app, 'd, ID> for Handle<'app, 'o, 'q, 'e, 'b, 'c, 'd, B, ID>
{
    type Error = Failure<Infallible, mailbox::Error<'d, ID>>;

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

impl<'app, 'o, 'q, 'e, 'b, 'c, 'd: 'app, B: ?Sized, ID: Identity + 'd>
    process::Process<'app, 'd, ID> for Handle<'app, 'o, 'q, 'e, 'b, 'c, 'd, B, ID>
{
    fn process_observed<'r, T, E, F, O>(
        &'r self,
        method: Method,
        path: &'r str,
        headers: &'r [(&'r str, &'r str)],
        body: &'r [u8],
        observe: O,
        process: F,
    ) -> impl abi::Fiber<'d, Output = Result<Result<T, E>, Self::Error>> + 'r
    where
        O: FnMut(&Self::Error) + 'r,
        F: FnOnce(
                reply::Scoped<'app, 'd>,
                budget::Check<'d>,
                NonZero<u8>,
            ) -> Result<T, process::Error<E>>
            + 'r,
        T: 'r,
        E: 'r,
    {
        self.call(method, path, headers, body)
            .process(observe, process)
    }
}

impl<'app, 'o, 'q, 'e, 'b, 'c, 'd: 'app, B: ?Sized, ID: Identity>
    Handle<'app, 'o, 'q, 'e, 'b, 'c, 'd, B, ID>
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
}

impl<'app, 'o, 'q, 'e, 'b, 'c, 'd: 'app, B: ?Sized, ID: Identity + 'd>
    Handle<'app, 'o, 'q, 'e, 'b, 'c, 'd, B, ID>
{
    pub fn get<'r>(
        &'r self,
        path: &'r str,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<reply::Scoped<'app, 'd>, Failure<Infallible, mailbox::Error<'d, ID>>>,
    > + 'r {
        self.send(Method::GET, path, &[])
    }

    pub fn send<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        body: &'r [u8],
    ) -> impl abi::Fiber<
        'd,
        Output = Result<reply::Scoped<'app, 'd>, Failure<Infallible, mailbox::Error<'d, ID>>>,
    > + 'r {
        self.send_with(method, path, &[], body)
    }

    pub fn retain<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        body: &'r [u8],
    ) -> impl abi::Fiber<
        'd,
        Output = Result<reply::Retained, Failure<Infallible, mailbox::Error<'d, ID>>>,
    > + 'r {
        self.call(method, path, &[], body).retained()
    }

    pub fn collect<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        body: &'r [u8],
    ) -> impl abi::Fiber<
        'd,
        Output = Result<reply::Contiguous, Failure<Infallible, mailbox::Error<'d, ID>>>,
    > + 'r {
        self.collect_with(method, path, &[], body)
    }

    pub fn collect_with<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        headers: &'r [(&'r str, &'r str)],
        body: &'r [u8],
    ) -> impl abi::Fiber<
        'd,
        Output = Result<reply::Contiguous, Failure<Infallible, mailbox::Error<'d, ID>>>,
    > + 'r {
        self.call(method, path, headers, body).contiguous()
    }

    pub fn stream<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        body: &'r [u8],
    ) -> impl abi::Fiber<
        'd,
        Output = Result<
            crate::client::Stream<'app, 'e, 'b, 'c, 'd, B, ID>,
            Failure<Infallible, mailbox::Error<'d, ID>>,
        >,
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
        Output = Result<
            crate::client::Stream<'app, 'e, 'b, 'c, 'd, B, ID>,
            Failure<Infallible, mailbox::Error<'d, ID>>,
        >,
    > + 'r {
        self.call(method, path, headers, body).stream()
    }

    pub fn stream_get<'r>(
        &'r self,
        path: &'r str,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<
            crate::client::Stream<'app, 'e, 'b, 'c, 'd, B, ID>,
            Failure<Infallible, mailbox::Error<'d, ID>>,
        >,
    > + 'r {
        self.stream(Method::GET, path, &[])
    }
}
