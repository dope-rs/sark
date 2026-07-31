use super::{Response, Serve, Shape};

pub trait IntoResponseShape<'req>: Sized {
    type Shape: Shape<'req>;

    fn into_response_shape(self) -> Self::Shape;
}

impl<'req> IntoResponseShape<'req> for Response {
    type Shape = Serve<'req>;

    fn into_response_shape(self) -> Self::Shape {
        self.into()
    }
}

impl super::super::__private::OwnedResponse for Response {}
impl super::super::__private::OwnedResponse for super::Chunked {}
