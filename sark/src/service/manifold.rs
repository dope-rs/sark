use dope_fiber::abi::Fiber;
use sark_core::http::body_kind::ResponseKind;
use sark_core::http::{IntoResponseShape, Shape, ShapeKind, ShapeMetadata, ShapeStream};

pub use sark_core::http::{StreamShape as NativeStream, SyncShape as Sync};

use super::spec::RouteSpec;
use crate::request;

pub struct NativeFiber;

pub trait NativeResponse<'req>: Sized {
    type Kind;
    type Shape: Shape<'req>;
    type Stream: 'static;

    const BODY_KIND: ResponseKind =
        <<Self::Shape as Shape<'req>>::Metadata as ShapeMetadata>::BODY_KIND;

    fn into_route_response(self) -> Self::Shape;
}

impl<'req, R> NativeResponse<'req> for R
where
    R: IntoResponseShape<'req>,
{
    type Kind = ShapeKind<'req, R::Shape>;
    type Shape = R::Shape;
    type Stream = ShapeStream<'req, R::Shape>;

    fn into_route_response(self) -> Self::Shape {
        self.into_response_shape()
    }
}

pub trait Route<State>: RouteSpec {
    fn invoke<'req, 'a>(
        params: <Self as RouteSpec>::Params<'req>,
        req: &request::Ref<'req>,
        headers: <Self as RouteSpec>::Headers<'req>,
        parsed_body: <Self as RouteSpec>::ParsedBody<'req>,
        state: &'a State,
    ) -> <Self as RouteSpec>::Response<'req>
    where
        'req: 'a;
}

pub trait TaskRoute<'d, State>: RouteSpec + Sized {
    fn invoke_task<'req>(
        params: <Self as RouteSpec>::Params<'req>,
        req: request::Ref<'req>,
        headers: <Self as RouteSpec>::Headers<'req>,
        parsed_body: <Self as RouteSpec>::ParsedBody<'req>,
        state: &'req State,
        timer: &'req crate::Timer<'d>,
    ) -> impl Fiber<'d, Output = Self::AsyncResponse> + 'req
    where
        State: 'req,
        'd: 'req;
}
