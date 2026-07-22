use super::super::__private::GeneratedResponse;
use super::{Chunked, Response, Serve, Shape};

pub trait OwnedShape: 'static {
    type Shape: Shape<'static>;

    const BODY_KIND: super::super::body_kind::ResponseKind;

    fn into_shape(self) -> Self::Shape;
}

impl<T> OwnedShape for T
where
    T: GeneratedResponse,
{
    type Shape = T::Shape;

    const BODY_KIND: super::super::body_kind::ResponseKind = T::BODY_KIND;

    fn into_shape(self) -> Self::Shape {
        self.into_owned_shape()
    }
}

impl OwnedShape for Response {
    type Shape = Serve<'static>;

    const BODY_KIND: super::super::body_kind::ResponseKind =
        super::super::body_kind::ResponseKind::Inline;

    fn into_shape(self) -> Self::Shape {
        self.into()
    }
}

impl OwnedShape for Chunked {
    type Shape = Self;

    const BODY_KIND: super::super::body_kind::ResponseKind =
        super::super::body_kind::ResponseKind::Inline;

    fn into_shape(self) -> Self::Shape {
        self
    }
}
