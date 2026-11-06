use std::mem;

use sark_http::request::body::Ref;

use crate::{body, duplex, manifold, request, request::frame::proof, route};

/// A route call whose body has already passed its request projection.
#[repr(C)]
pub struct Call<H, B> {
    request: H,
    body: B,
}

impl<H, B> Call<H, B> {
    #[doc(hidden)]
    pub const fn new(request: H, body: B) -> Self {
        Self { request, body }
    }

    #[doc(hidden)]
    pub fn into_parts(self) -> (H, B) {
        (self.request, self.body)
    }
}

impl<'req, 'd: 'req, R: route::Project>
    Call<
        request::raw::Parsed<request::Head<&'req [u8], R::Frame>, R>,
        body::Scoped<'req, 'd, R::Body>,
    >
{
    pub fn try_invoke<'state, T, S>(
        self,
        state: &'state S,
    ) -> Result<T::Response<'req, 'd>, <R as route::Project>::Error<'req>>
    where
        T: route::Spec<Request = R> + manifold::Route<S>,
        'req: 'state,
    {
        let (params, request, headers, body) = self.try_parse()?;
        Ok(T::invoke((params, &request, headers, body, state)))
    }

    #[doc(hidden)]
    pub fn try_open<'state, T, S>(
        self,
        state: &'state S,
    ) -> Result<T::Session, <R as route::Project>::Error<'req>>
    where
        T: route::Spec<Request = R> + duplex::Route<'state, S>,
        T::Session: duplex::Session<'d>,
        'd: 'state,
    {
        let (params, request, headers, body) = self.try_parse()?;
        Ok(T::open(params, &request, headers, body, state))
    }

    fn try_parse(self) -> Result<Parts<'req, 'd, R>, <R as route::Project>::Error<'req>> {
        let body = self.body.into_inner();
        let view = body.into();
        let params = R::from_params(&self.request)?;
        let headers = R::from_headers(&self.request)?;
        let body = R::parse_body(body)?;
        let request = request::View::from_head(self.request.into_owner(), view);
        Ok((params, request, headers, body))
    }
}

const _: () = assert!(
    mem::size_of::<Call<request::Head<&'static [u8]>, Ref<'static, 'static>>>()
        == mem::size_of::<request::View<'static, 'static>>()
);
const _: () = assert!(
    mem::size_of::<
        Call<
            request::Head<&'static [u8], proof::Peer>,
            body::Scoped<'static, 'static, body::Contiguous>,
        >,
    >() < mem::size_of::<request::View<'static, 'static, proof::Peer>>()
);

type Parts<'req, 'd, R> = (
    <R as route::Project>::Params<'req>,
    request::View<'req, 'd, <R as route::Schema>::Frame>,
    <R as route::Project>::Headers<'req>,
    <R as route::Project>::ParsedBody<'req, 'd>,
);
