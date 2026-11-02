use sark_core::route;

/// Route parameters bound to the exact input that produced them.
pub struct Bound<S, R: route::Spec> {
    pub(in crate::wire::parsing) head: S,
    pub(in crate::wire::parsing) params: <R::Request as route::Schema>::RawParams,
}

impl<S, R: route::Spec> Bound<S, R> {
    /// # Safety
    /// `params` must result from matching `R` against this exact head.
    pub unsafe fn new(head: S, params: <R::Request as route::Schema>::RawParams) -> Self {
        Self { head, params }
    }

    pub(super) fn retained(head: S, params: <R::Request as route::Schema>::RawParams) -> Self {
        Self { head, params }
    }
}

impl<S, R: route::Spec<Request: route::Schema<RawParams = ()>>> Bound<S, R> {
    pub fn empty(head: S) -> Self {
        Self { head, params: () }
    }
}
