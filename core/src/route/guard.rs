use std::ops::ControlFlow;

use o3::buffer::resident::bound;
use sark_http::{method::Method, payload::response::dispatch, request::Form};

/// A protocol-neutral view of a routed request head.
#[derive(Clone, Copy)]
pub struct Head<'req> {
    method: &'req Method,
    form: Form,
    path: Option<&'req [u8]>,
    authority: Option<&'req [u8]>,
}

impl<'req> Head<'req> {
    #[doc(hidden)]
    pub const fn from_parts(
        method: &'req Method,
        form: Form,
        path: Option<&'req [u8]>,
        host: Option<&'req [u8]>,
    ) -> Self {
        Self {
            method,
            form,
            path: match form {
                Form::Authority => None,
                Form::Path | Form::Root | Form::Asterisk | Form::Extended => path,
            },
            authority: host,
        }
    }

    pub const fn method(&self) -> &'req Method {
        self.method
    }

    pub const fn form(&self) -> Form {
        self.form
    }

    /// Authority-form requests have no path; asterisk-form requests retain `*`.
    pub const fn path(&self) -> Option<&'req [u8]> {
        self.path
    }

    pub const fn authority(&self) -> Option<&'req [u8]> {
        self.authority
    }
}

/// Synchronous policy evaluated after routing and before request-body admission.
pub trait Guard<S> {
    type Egress<'d>: bound::Bound + 'd;

    type Break<'req, 'd>: dispatch::IntoOutput<
            'req,
            'd,
            Output: dispatch::Emit<'req, 'd, Stream = dispatch::Never>
                        + dispatch::Immediate<'req, 'd>,
        >
    where
        'd: 'req,
        S: 'req;

    fn check<'req, 'd>(head: Head<'req>, state: &'req S) -> ControlFlow<Self::Break<'req, 'd>>
    where
        'd: 'req,
        S: 'req;
}

/// Protocol response staging for one guard chain.
/// A rejection consumes it, keeping reservation and emission linear without
/// type erasure.
#[doc(hidden)]
pub trait Reject<'req, 'd: 'req>: Sized {
    type Break;

    fn try_reject<R>(self, response: R) -> Self::Break
    where
        R: dispatch::Immediate<'req, 'd>;
}

impl<'req, 'd: 'req, E: dispatch::Accept<'d>> Reject<'req, 'd> for E {
    type Break = Result<E::Output, E::Error>;

    fn try_reject<R>(self, response: R) -> Self::Break
    where
        R: dispatch::Immediate<'req, 'd>,
    {
        dispatch::Immediate::emit(response, self)
    }
}

/// Guard chain for one generated route occurrence.
/// Occurrence ownership permits one route type under independently guarded
/// scopes.
#[doc(hidden)]
pub trait Slot<S> {
    fn try_admit<'req, 'd, R>(
        head: Head<'req>,
        state: &'req S,
        reject: R,
    ) -> ControlFlow<R::Break, R>
    where
        'd: 'req,
        S: 'req,
        R: Reject<'req, 'd>;
}
