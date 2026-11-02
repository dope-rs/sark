use std::task;

use dope_fiber::{abi, context};
use o3::{base::cell::region, buffer::resident};

use crate::{
    client::{manifold::handle, receive},
    metadata,
};

type Received<'app, 'd, P, D> = handle::Delivery<
    'app,
    'd,
    P,
    receive::Decoded<<D as crate::Codec>::Owned<resident::Shared<'d>>>,
>;

pub trait Request<'app, 'd: 'app, P: ?Sized + 'app>: Sized {
    type Error;

    fn send<'a, D: crate::Codec + 'a>(
        &'a mut self,
        codec: D,
        message: &'a D::Encode,
    ) -> impl abi::Fiber<'d, Output = Result<(), Self::Error>> + 'a
    where
        'd: 'a;

    fn finish<'a, D: crate::Codec + 'a>(
        self,
        codec: D,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<handle::Delivery<'app, 'd, P, D::Owned<resident::Shared<'d>>>, Self::Error>,
    > + 'a
    where
        'd: 'a,
        Self: 'a;
}

pub trait Recv<'app, 'd: 'app, P: ?Sized + 'app>: Sized {
    type Error;

    fn recv<'a, D: crate::Codec + 'a>(
        &'a mut self,
        codec: D,
    ) -> impl abi::Fiber<'d, Output = Result<Received<'app, 'd, P, D>, Self::Error>> + 'a
    where
        'd: 'a;
}

pub trait Duplex<'app, 'd: 'app, P: ?Sized + 'app>: Recv<'app, 'd, P> {
    type Response: Recv<'app, 'd, P, Error = Self::Error>;
    type SendHalf: Write<'app, 'd, P, Error = Self::Error>;

    fn send<'a, D: crate::Codec + 'a>(
        &'a mut self,
        codec: D,
        message: &'a D::Encode,
    ) -> impl abi::Fiber<'d, Output = Result<(), Self::Error>> + 'a
    where
        'd: 'a;

    fn try_finish(
        self,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> Result<Self::Response, Self::Error>;

    /// Closes the request half on its driver's next poll.
    fn finish<'a>(self) -> impl abi::Fiber<'d, Output = Result<Self::Response, Self::Error>> + 'a
    where
        'd: 'a,
        Self: 'a,
    {
        with_driver(move |token| self.try_finish(token))
    }

    fn split(self) -> Result<(Self::SendHalf, Self::Response), Self::Error>;
}

pub trait Write<'app, 'd: 'app, P: ?Sized + 'app>: Sized {
    type Error;

    fn send<'a, D: crate::Codec + 'a>(
        &'a mut self,
        codec: D,
        message: &'a D::Encode,
    ) -> impl abi::Fiber<'d, Output = Result<(), Self::Error>> + 'a
    where
        'd: 'a;

    fn try_finish(
        self,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> Result<(), Self::Error>;

    /// Closes the request half on its driver's next poll.
    fn finish<'a>(self) -> impl abi::Fiber<'d, Output = Result<(), Self::Error>> + 'a
    where
        'd: 'a,
        Self: 'a,
    {
        with_driver(move |token| self.try_finish(token))
    }
}

/// The provided client has no implicit deadline and does not emit `grpc-timeout`.
/// Bound the whole call with [`dope_fiber::task::time::budget::Budget::timeout`].
/// Dropping an unfinished call or stream schedules cancellation; timing out
/// a borrowed receive alone leaves the stream owned by its caller.
pub trait Session<'app, 'd: 'app, P: ?Sized + 'app> {
    type Error;
    type Request: Request<'app, 'd, P, Error = Self::Error>;
    type Response: Recv<'app, 'd, P, Error = Self::Error>;
    type Bidi: Duplex<'app, 'd, P, Error = Self::Error, Response = Self::Response>;

    fn try_request(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        path: &[u8],
        host: Option<&[u8]>,
        metadata: &metadata::Metadata,
    ) -> Result<Self::Request, Self::Error>;

    fn try_bidi(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        path: &[u8],
        host: Option<&[u8]>,
        metadata: &metadata::Metadata,
    ) -> Result<Self::Bidi, Self::Error>;

    /// Opens a request on its driver's next poll, preserving admission errors.
    fn open_request<'a>(
        &'a self,
        path: &'a [u8],
        host: Option<&'a [u8]>,
        metadata: &'a metadata::Metadata,
    ) -> impl abi::Fiber<'d, Output = Result<Self::Request, Self::Error>> + 'a
    where
        'd: 'a,
    {
        with_driver(move |token| self.try_request(token, path, host, metadata))
    }

    /// Opens a bidirectional request on its driver's next poll, preserving admission errors.
    fn open_bidi<'a>(
        &'a self,
        path: &'a [u8],
        host: Option<&'a [u8]>,
        metadata: &'a metadata::Metadata,
    ) -> impl abi::Fiber<'d, Output = Result<Self::Bidi, Self::Error>> + 'a
    where
        'd: 'a,
    {
        with_driver(move |token| self.try_bidi(token, path, host, metadata))
    }

    fn unary<'a, D: crate::Codec + 'a>(
        &'a self,
        path: &'a [u8],
        host: Option<&'a [u8]>,
        metadata: &'a metadata::Metadata,
        codec: D,
        message: &'a D::Encode,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<handle::Delivery<'app, 'd, P, D::Owned<resident::Shared<'d>>>, Self::Error>,
    > + 'a
    where
        'd: 'a;

    fn stream<'a, D: crate::Codec + 'a>(
        &'a self,
        path: &'a [u8],
        host: Option<&'a [u8]>,
        metadata: &'a metadata::Metadata,
        codec: D,
        message: &'a D::Encode,
    ) -> impl abi::Fiber<'d, Output = Result<Self::Response, Self::Error>> + 'a
    where
        'd: 'a;
}

fn with_driver<'a, 'd: 'a, T>(
    run: impl FnOnce(&mut region::Token<region::Identity<'d, 1>>) -> T + 'a,
) -> impl abi::Fiber<'d, Output = T> + 'a {
    let mut run = Some(run);
    abi::PollFn::admitted(move |mut admit: context::Admission<'_, '_, 'd>| {
        let Some(run) = run.take() else {
            return task::Poll::Pending;
        };
        task::Poll::Ready(run(admit.context().driver().state_token()))
    })
}
