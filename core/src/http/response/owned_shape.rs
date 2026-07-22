use std::rc::Rc;

use http::{HeaderName, HeaderValue, StatusCode};
use o3::buffer::{Bytes, Owned, Retained, Shared};

use super::super::__private::{self, GeneratedResponse, OwnedValue};
use super::{Chunked, InlineHeaderValue, Response, Serve, Shape};

pub trait OwnedShape: super::super::__private::OwnedShape + 'static {
    type Shape: Shape<'static>;

    const BODY_KIND: super::super::body_kind::ResponseKind;

    fn into_shape(self) -> Self::Shape;
}

impl<T> __private::OwnedShape for T
where
    T: GeneratedResponse,
    T::Fields: OwnedValue,
{
}

impl<T> OwnedShape for T
where
    T: GeneratedResponse,
    T::Fields: OwnedValue,
{
    type Shape = T::Shape;

    const BODY_KIND: super::super::body_kind::ResponseKind = T::BODY_KIND;

    fn into_shape(self) -> Self::Shape {
        self.into_owned_shape()
    }
}

impl super::super::__private::OwnedShape for Response {}

impl OwnedShape for Response {
    type Shape = Serve;

    const BODY_KIND: super::super::body_kind::ResponseKind =
        super::super::body_kind::ResponseKind::Inline;

    fn into_shape(self) -> Self::Shape {
        self.into()
    }
}

impl super::super::__private::OwnedShape for Chunked {}

impl OwnedShape for Chunked {
    type Shape = Self;

    const BODY_KIND: super::super::body_kind::ResponseKind =
        super::super::body_kind::ResponseKind::Inline;

    fn into_shape(self) -> Self::Shape {
        self
    }
}

impl OwnedValue for bool {}
impl OwnedValue for char {}
impl OwnedValue for f32 {}
impl OwnedValue for f64 {}
impl OwnedValue for i8 {}
impl OwnedValue for i16 {}
impl OwnedValue for i32 {}
impl OwnedValue for i64 {}
impl OwnedValue for i128 {}
impl OwnedValue for isize {}
impl OwnedValue for u8 {}
impl OwnedValue for u16 {}
impl OwnedValue for u32 {}
impl OwnedValue for u64 {}
impl OwnedValue for u128 {}
impl OwnedValue for usize {}
impl OwnedValue for HeaderName {}
impl OwnedValue for HeaderValue {}
impl OwnedValue for InlineHeaderValue {}
impl OwnedValue for Owned {}
impl OwnedValue for Shared {}
impl OwnedValue for StatusCode {}
impl OwnedValue for String {}
impl OwnedValue for Bytes<Retained> {}

impl<T: ?Sized> OwnedValue for &'static T {}
impl<T: OwnedValue, const N: usize> OwnedValue for [T; N] {}
impl<T: OwnedValue> OwnedValue for Box<T> {}
impl<T: OwnedValue> OwnedValue for Option<T> {}
impl<T: OwnedValue> OwnedValue for Rc<T> {}
impl<T: OwnedValue> OwnedValue for Vec<T> {}
impl<T: OwnedValue, E: OwnedValue> OwnedValue for Result<T, E> {}

macro_rules! owned_tuples {
    ($(($($ty:ident),+)),+ $(,)?) => {
        $(impl<$($ty: OwnedValue),+> OwnedValue for ($($ty,)+) {})+
    };
}

owned_tuples! {
    (T0),
    (T0, T1),
    (T0, T1, T2),
    (T0, T1, T2, T3),
    (T0, T1, T2, T3, T4),
    (T0, T1, T2, T3, T4, T5),
    (T0, T1, T2, T3, T4, T5, T6),
    (T0, T1, T2, T3, T4, T5, T6, T7),
    (T0, T1, T2, T3, T4, T5, T6, T7, T8),
    (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9),
    (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10),
    (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11),
    (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12),
    (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13),
    (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14),
    (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15),
}
