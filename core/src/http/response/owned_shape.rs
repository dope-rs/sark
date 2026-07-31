use super::super::__private::OwnedResponse;
use super::{IntoResponseShape, Shape, ShapeMetadata};

pub trait OwnedShape: 'static {
    type Shape: Shape<'static>;

    const BODY_KIND: super::super::body_kind::ResponseKind =
        <<Self::Shape as Shape<'static>>::Metadata as ShapeMetadata>::BODY_KIND;

    fn into_shape(self) -> Self::Shape;
}

impl<T> OwnedShape for T
where
    T: OwnedResponse,
{
    type Shape = <T as IntoResponseShape<'static>>::Shape;

    fn into_shape(self) -> Self::Shape {
        self.into_response_shape()
    }
}
