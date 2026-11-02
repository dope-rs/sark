//! Compile-time request storage forms.

use core::marker;

use sark_http::request::Bytes;

/// Storage family selected while generated request fields are monomorphized.
#[doc(hidden)]
pub trait Mode<'d> {
    type Bytes;
}

/// Request fields retained by one driver domain.
#[doc(hidden)]
pub enum Own {}

/// Request fields borrowed for one synchronous invocation.
#[doc(hidden)]
pub struct Ref<'req> {
    lifetime: marker::PhantomData<&'req mut &'req ()>,
}

impl<'d> Mode<'d> for Own {
    type Bytes = Bytes<'d>;
}

impl<'req, 'd: 'req> Mode<'d> for Ref<'req> {
    type Bytes = &'req [u8];
}
