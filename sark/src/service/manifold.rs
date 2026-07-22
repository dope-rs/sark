use dope_fiber::{Fiber, Ready};
use o3::buffer::Shared;
use sark_core::http::{
    Chunked, FixedResponseInner, IntoServeResponse, MonoResponseInner, Response, ServeInner, Shape,
    StaticResponseInner, Stream,
};

use super::spec::RouteSpec;
use crate::request;

pub struct Sync;
pub struct NativeFiber;
pub struct NativeStream;

pub trait InvokeKind<R: RouteSpec> {
    type Output;
}

impl<R: RouteSpec> InvokeKind<R> for Sync {
    type Output = ();
}

impl<R: RouteSpec> InvokeKind<R> for NativeFiber {
    type Output = R::AsyncResponse;
}

impl<R: RouteSpec> InvokeKind<R> for NativeStream {
    type Output = ();
}

pub fn ready() -> Ready<()> {
    dope_fiber::ready(())
}

pub trait NativeResponse<'req>: Sized {
    type Kind;
    type Shape: Shape<'req>;
    type Stream: 'static;

    const BODY_KIND: sark_core::http::body_kind::ResponseKind;

    fn into_route_response(self) -> Self::Shape;
}

macro_rules! native_response {
    ($ty:ty, $body_kind:ident) => {
        impl<'req> NativeResponse<'req> for $ty {
            type Kind = Sync;
            type Shape = Self;
            type Stream = sark_core::http::NeverStream;

            const BODY_KIND: sark_core::http::body_kind::ResponseKind =
                sark_core::http::body_kind::ResponseKind::$body_kind;

            fn into_route_response(self) -> Self::Shape {
                self
            }
        }
    };
}

impl<'req, S> NativeResponse<'req> for Stream<S>
where
    S: 'static,
{
    type Kind = NativeStream;
    type Shape = Self;
    type Stream = S;

    const BODY_KIND: sark_core::http::body_kind::ResponseKind =
        sark_core::http::body_kind::ResponseKind::Stream;

    fn into_route_response(self) -> Self::Shape {
        self
    }
}

native_response!(Chunked, Inline);

impl<'req, const N: usize> NativeResponse<'req> for ServeInner<'req, N> {
    type Kind = Sync;
    type Shape = Self;
    type Stream = sark_core::http::NeverStream;

    const BODY_KIND: sark_core::http::body_kind::ResponseKind =
        sark_core::http::body_kind::ResponseKind::Inline;

    fn into_route_response(self) -> Self::Shape {
        self
    }
}

impl<'req, const N: usize> NativeResponse<'req> for FixedResponseInner<'req, N> {
    type Kind = Sync;
    type Shape = Self;
    type Stream = sark_core::http::NeverStream;

    const BODY_KIND: sark_core::http::body_kind::ResponseKind =
        sark_core::http::body_kind::ResponseKind::Inline;

    fn into_route_response(self) -> Self::Shape {
        self
    }
}

impl<'req, const N: usize> NativeResponse<'req> for MonoResponseInner<'req, N> {
    type Kind = Sync;
    type Shape = Self;
    type Stream = sark_core::http::NeverStream;

    const BODY_KIND: sark_core::http::body_kind::ResponseKind =
        sark_core::http::body_kind::ResponseKind::Inline;

    fn into_route_response(self) -> Self::Shape {
        self
    }
}

impl<'req, const N: usize> NativeResponse<'req> for StaticResponseInner<'req, N> {
    type Kind = Sync;
    type Shape = Self;
    type Stream = sark_core::http::NeverStream;

    const BODY_KIND: sark_core::http::body_kind::ResponseKind =
        sark_core::http::body_kind::ResponseKind::Static;

    fn into_route_response(self) -> Self::Shape {
        self
    }
}

impl<'req> NativeResponse<'req> for Response {
    type Kind = Sync;
    type Shape = ServeInner<'req>;
    type Stream = sark_core::http::NeverStream;

    const BODY_KIND: sark_core::http::body_kind::ResponseKind =
        sark_core::http::body_kind::ResponseKind::Inline;

    fn into_route_response(self) -> Self::Shape {
        IntoServeResponse::into_serve_response(self)
    }
}

pub trait Kind<'d, R, F>
where
    R: RouteSpec,
{
    type Owner;
    type Task: Fiber<'d, Output = Self::Output> + 'd;
    type Output;

    const STREAM: bool;
}

impl<'d, R, F> Kind<'d, R, F> for Sync
where
    R: RouteSpec,
{
    type Owner = ();
    type Task = Ready<()>;
    type Output = ();

    const STREAM: bool = false;
}

impl<'d, R, F> Kind<'d, R, F> for NativeFiber
where
    R: RouteSpec,
    F: Fiber<'d, Output = R::AsyncResponse> + 'd,
{
    type Owner = request::RequestStorage;
    type Task = F;
    type Output = R::AsyncResponse;

    const STREAM: bool = false;
}

impl<'d, R, F> Kind<'d, R, F> for NativeStream
where
    R: RouteSpec,
    R::Stream: Fiber<'d, Output = Option<Shared>> + 'd,
{
    type Owner = ();
    type Task = R::Stream;
    type Output = Option<Shared>;

    const STREAM: bool = true;
}

pub trait Route<State>: RouteSpec {
    fn invoke<'req, 'a>(
        &'a self,
        params: <Self as RouteSpec>::Params<'req>,
        req: &request::Ref<'req>,
        headers: <Self as RouteSpec>::Headers<'req>,
        parsed_body: <Self as RouteSpec>::ParsedBody<'req>,
        state: &'a State,
    ) -> <Self as RouteSpec>::Response<'req>
    where
        'req: 'a;
}

pub trait TaskRoute<'d, State>: RouteSpec + Sized
where
    Self::Kind: InvokeKind<Self>,
{
    fn invoke_task<'req>(
        &'req self,
        params: <Self as RouteSpec>::Params<'req>,
        req: request::Ref<'req>,
        headers: <Self as RouteSpec>::Headers<'req>,
        parsed_body: <Self as RouteSpec>::ParsedBody<'req>,
        state: &'req State,
        timer: &'req crate::Timer<'d>,
    ) -> impl Fiber<'d, Output = <Self::Kind as InvokeKind<Self>>::Output> + 'req
    where
        State: 'req,
        'd: 'req;
}
